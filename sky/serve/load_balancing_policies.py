"""LoadBalancingPolicy: Policy to select endpoint."""
import random
import typing
from typing import List, Optional
from threading import Lock
import time

from sky import sky_logging

if typing.TYPE_CHECKING:
    import fastapi

logger = sky_logging.init_logger(__name__)


class LoadBalancingPolicy:
    """Abstract class for load balancing policies."""

    def __init__(self) -> None:
        self.ready_replicas: List[str] = []
        # Lock for thread-safe operations
        self.lock = Lock()

    def set_ready_replicas(self, ready_replicas: List[str]) -> None:
        raise NotImplementedError

    # TODO(tian): We should have an abstract class for Request to
    # compatible with all frameworks.
    def select_replica(self, request: 'fastapi.Request') -> Optional[str]:
        raise NotImplementedError


class RoundRobinPolicy(LoadBalancingPolicy):
    """Round-robin load balancing policy."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.index = 0

    def set_ready_replicas(self, ready_replicas: List[str]) -> None:
        # Acquire the lock before modifying shared state
        with self.lock:
            if set(ready_replicas) != set(self.ready_replicas):
                # If the autoscaler keeps scaling up and down the replicas,
                # we need this shuffle to not let the first replica have the
                # most of the load.
                random.shuffle(ready_replicas)
                self.ready_replicas = ready_replicas
                self.index = 0

    def select_replica(self, request: 'fastapi.Request') -> Optional[str]:
        with self.lock:
            if not self.ready_replicas:
                return None
            ready_replica_url = self.ready_replicas[self.index]
            self.index = (self.index + 1) % len(self.ready_replicas)

        request_repr = ('<Request '
                        f'method="{request.method}" '
                        f'url="{request.url}" '
                        f'headers={dict(request.headers)} '
                        f'query_params={dict(request.query_params)}'
                        '>')
        logger.info(f'Selected replica {ready_replica_url} '
                    f'for request {request_repr}')
        return ready_replica_url


class LeastConnectionsPolicy(LoadBalancingPolicy):
    """Least Connections load balancing policy.
    This policy selects the replica with the least number of connections.
    TODO(Ping Zhang): Logging Efficiency
     """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.connections_count = {}

    def set_ready_replicas(self, ready_replicas: List[str]) -> None:
        # Initialize connection count for new replicas and remove old ones
        with self.lock:
            new_connections_count = {replica: self.connections_count.get(replica, 0) for replica in ready_replicas}
            self.ready_replicas = ready_replicas
            self.connections_count = new_connections_count

    def select_replica(self, request: 'fastapi.Request') -> Optional[str]:
        with self.lock:
            if not self.ready_replicas:
                return None
            # Select the replica with the least number of connections
            # TODO(Ping Zhang): use min-heap to optimize the performance
            selected_replica = min(self.ready_replicas, key=lambda replica: self.connections_count[replica])
            
            # Increment the connection count for the selected replica
            self.connections_count[selected_replica] += 1

        # Log the request and selected replica
        request_repr = ('<Request '
                        f'method="{request.method}" '
                        f'url="{request.url}" '
                        f'headers={dict(request.headers)} '
                        f'query_params={dict(request.query_params)}'
                        '>')
        logger.info(f'Selected replica {selected_replica} '
                    f'for request {request_repr}')

        return selected_replica

    def release_connection(self, replica: str) -> None:
        # Decrement the connection count for a replica when the connection is closed
        if replica in self.connections_count:
            self.connections_count[replica] = max(0, self.connections_count[replica] - 1)


class EwmaPolicy(LoadBalancingPolicy):
    """Exponentially Weighted Moving Average load balancing policy.
    This policy selects the replica with the least EWMA.
    Note that the LLMs (TGI instances) are serving in a streaming manner, which means
    tokens are responsed to the client as soon as they are generated. Thus, the EWMA
    policy should carefully consider the time cost evaluation among different replicas.
    """
    
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.ewma_scores = {}
        self.last_response_time = {}
        # Alpha is the factor that controls the decay rate of the previous EWMA values
        self.alpha = 0.5  

    def set_ready_replicas(self, ready_replicas: List[str]) -> None:
        # Initialize EWMA scores and response times for new replicas
        new_ewma_scores = {replica: self.ewma_scores.get(replica, 0) for replica in ready_replicas}
        new_last_response_time = {replica: self.last_response_time.get(replica, 0) for replica in ready_replicas}
        self.ready_replicas = ready_replicas
        self.ewma_scores = new_ewma_scores
        self.last_response_time = new_last_response_time

    def select_replica(self, request: 'fastapi.Request') -> Optional[str]:
        if not self.ready_replicas:
            return None
        
        # Select the replica with the least EWMA score
        selected_replica = min(self.ready_replicas, key=lambda replica: self.ewma_scores[replica])
        
        # Record the start time of the request
        self.last_response_time[selected_replica] = time.time()

        request_repr = ('<Request '
                        f'method="{request.method}" '
                        f'url="{request.url}" '
                        f'headers={dict(request.headers)} '
                        f'query_params={dict(request.query_params)}'
                        '>')
        logger.info(f'Selected replica {selected_replica} '
                    f'for request {request_repr}')

        return selected_replica

    def update_ewma(self, replica: str) -> None:
        # Calculate the response time for the completed request
        # TODO(Ping Zhang): Maybe we need calibrate the unit of response time
        response_time = time.time() - self.last_response_time[replica]
        
        # Update the EWMA score for the replica
        # TODO(Ping Zhang): Maybe we should consider more past steps in the EWMA calculation
        if replica in self.ewma_scores:
            old_score = self.ewma_scores[replica]
            new_score = self.alpha * response_time + (1 - self.alpha) * old_score
            self.ewma_scores[replica] = new_score
        else:
            # If it's the first request for this replica, initialize the score with the response time
            self.ewma_scores[replica] = response_time


class LatencyAndQueueSizeWeightedPolicy(LoadBalancingPolicy):
    """Latency and instance queue size weighted load balancing policy.
    This policy selects the replica with the following formular:
    min(w1*latency + w2*queue_size)
    e.g., w1 = 1.0, w2 = 3.0
    """
    
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.ewma_scores = {}
        # Track the number of ongoing requests for each replica
        self.ongoing_requests = {}
        # Alpha is the factor that controls the decay rate of the previous EWMA values  
        self.alpha = 0.5  

    def set_ready_replicas(self, ready_replicas: List[str]) -> None:
        # Initialize EWMA scores and ongoing requests for new replicas
        new_ewma_scores = {replica: self.ewma_scores.get(replica, 0) for replica in ready_replicas}
        new_ongoing_requests = {replica: self.ongoing_requests.get(replica, 0) for replica in ready_replicas}
        self.ready_replicas = ready_replicas
        self.ewma_scores = new_ewma_scores
        self.ongoing_requests = new_ongoing_requests

    def select_replica(self, request: 'fastapi.Request') -> Optional[str]:
        if not self.ready_replicas:
            return None
        
        # Select the replica with the least (EWMA score + ongoing requests factor)
        # selected_replica = min(self.ready_replicas, key=lambda replica: self.ewma_scores[replica] + self.ongoing_requests[replica])
        
        weight_ewma = 1.0
        # assume the execution time of the request is 3.0s
        # this value, to some extent, is used for an estimation of the request execution time in the queue
        weight_ongoing = 3.0  

        # select the replica with the least (EWMA score + ongoing requests factor)
        selected_replica = min(
            self.ready_replicas,
            key=lambda replica: weight_ewma * self.ewma_scores[replica] + weight_ongoing * self.ongoing_requests[replica]
        )

        # Increment the count of ongoing requests for the selected replica
        self.ongoing_requests[selected_replica] += 1

        request_repr = ('<Request '
                        f'method="{request.method}" '
                        f'url="{request.url}" '
                        f'headers={dict(request.headers)} '
                        f'query_params={dict(request.query_params)}'
                        '>')
        logger.info(f'Selected replica {selected_replica} '
                    f'for request {request_repr}')

        return selected_replica

    def update_ewma(self, replica: str, response_time: float) -> None:
        # Decrement the count of ongoing requests for the replica
        self.ongoing_requests[replica] = max(0, self.ongoing_requests[replica] - 1)
        
        # Update the EWMA score for the replica
        # TODO(Ping Zhang): Maybe we should consider more past steps in the EWMA calculation
        if replica in self.ewma_scores:
            old_score = self.ewma_scores[replica]
            new_score = self.alpha * response_time + (1 - self.alpha) * old_score
            self.ewma_scores[replica] = new_score
        else:
            # If it's the first request for this replica, initialize the score with the response time
            self.ewma_scores[replica] = response_time


class LatencyAndQueueSizeNormalizedPolicy(LoadBalancingPolicy):
    """Latency and instance queue size weighted load balancing policy.
    This policy selects the replica with the following formular:
    min(Normalized latency + Normalized queue_size)
    """
    
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.ewma_scores = {}
        # Track the number of ongoing requests for each replica
        self.ongoing_requests = {}
        # Alpha is the factor that controls the decay rate of the previous EWMA values  
        self.alpha = 0.5  

    def normalize(self, values):
        max_value = max(values)
        min_value = min(values)
        if max_value == min_value:
            return [1.0] * len(values)
        return [(val - min_value) / (max_value - min_value) for val in values]
    
    def set_ready_replicas(self, ready_replicas: List[str]) -> None:
        # Initialize EWMA scores and ongoing requests for new replicas
        new_ewma_scores = {replica: self.ewma_scores.get(replica, 0) for replica in ready_replicas}
        new_ongoing_requests = {replica: self.ongoing_requests.get(replica, 0) for replica in ready_replicas}
        self.ready_replicas = ready_replicas
        self.ewma_scores = new_ewma_scores
        self.ongoing_requests = new_ongoing_requests

    def select_replica(self, request: 'fastapi.Request') -> Optional[str]:
        if not self.ready_replicas:
            return None

        # normalize ewma_scores and ongoing_requests
        replica_to_index = {replica: index for index, replica in enumerate(self.ready_replicas)}
        normalized_ewma_scores = self.normalize([self.ewma_scores[replica] for replica in self.ready_replicas])
        normalized_ongoing_requests = self.normalize([self.ongoing_requests[replica] for replica in self.ready_replicas])

        # select the replica with the least (Normalized latency + Normalized queue_size)
        selected_replica = min(
            self.ready_replicas,
            key=lambda replica: normalized_ewma_scores[replica_to_index[replica]] + normalized_ongoing_requests[replica_to_index[replica]]
        )

        # Increment the count of ongoing requests for the selected replica
        self.ongoing_requests[selected_replica] += 1

        request_repr = ('<Request '
                        f'method="{request.method}" '
                        f'url="{request.url}" '
                        f'headers={dict(request.headers)} '
                        f'query_params={dict(request.query_params)}'
                        '>')
        logger.info(f'Selected replica {selected_replica} '
                    f'for request {request_repr}')

        return selected_replica

    def update_ewma(self, replica: str, response_time: float) -> None:
        # Decrement the count of ongoing requests for the replica
        self.ongoing_requests[replica] = max(0, self.ongoing_requests[replica] - 1)
        
        # Update the EWMA score for the replica
        # TODO(Ping Zhang): Maybe we should consider more past steps in the EWMA calculation
        if replica in self.ewma_scores:
            old_score = self.ewma_scores[replica]
            new_score = self.alpha * response_time + (1 - self.alpha) * old_score
            self.ewma_scores[replica] = new_score
        else:
            # If it's the first request for this replica, initialize the score with the response time
            self.ewma_scores[replica] = response_time


class TotalStringLengthOfQueuePolicy(LoadBalancingPolicy):
    """Total String Length Of Queue load balancing policy.
    This policy selects the replica based on the total string length of all the requests in the queue.
    e.g., 
        
        replica 1: ongoing requets:          [req1, req2, req3], 
                   string length of queue:   [10, 20, 30], 
                   total string length:      60
        
        replica 2: ongoing requets:          [req4, req5, req6, req10], 
                   string length of queue:   [5, 12, 21, 4], 
                   total string length:      42
        
        replica 3: ongoing requets:          [req7, req8], 
                   string length of queue:   [50, 90], 
                   total string length:      140
    
    The policy selects replica 2.
    """
    
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Track the total string length in the queue for each replica
        # TODO(Ping Zhang): all the data structure should be thread safe, becasue we 
        # are (probably) going to increase and decrease the value in a parallel way.
        self.total_string_length = {}  

    def set_ready_replicas(self, ready_replicas: List[str]) -> None:
        # Initialize total string length for new replicas
        new_total_string_length = {replica: self.total_string_length.get(replica, 0) for replica in ready_replicas}
        self.ready_replicas = ready_replicas
        self.total_string_length = new_total_string_length

    def select_replica(self, request: 'fastapi.Request') -> Optional[str]:
        if not self.ready_replicas:
            return None
        
        # Select the replica with the minimum total string length in the queue
        selected_replica = min(self.ready_replicas, key=lambda replica: self.total_string_length[replica])

        # Increment the total string length for the selected replica by the length of the incoming request
        # Assuming the request has an attribute 'content' which is the string
        # TODO(Ping Zhang): 
        # 1. check the attribute of the request;
        # 2. check the type of the request.content;
        # 3. we should consider bad cases in future, e.g., very long input but very shor ouptut
        self.total_string_length[selected_replica] += len(request.content)

        request_repr = ('<Request '
                        f'method="{request.method}" '
                        f'url="{request.url}" '
                        f'headers={dict(request.headers)} '
                        f'query_params={dict(request.query_params)} '
                        f'content_length={len(request.content)}'
                        '>')
        logger.info(f'Selected replica {selected_replica} '
                    f'for request {request_repr}')

        return selected_replica

    def release_request(self, replica: str, request: 'fastapi.Request') -> None:
        # Decrement the total string length for the replica when a request is completed
        if replica in self.total_string_length:
            self.total_string_length[replica] = max(0, self.total_string_length[replica] - len(request.content))


# TODO
class TotalQueueTimePolicy(LoadBalancingPolicy):
    """Total Queue Time load balancing policy.
    This policy selects the replica based on the total queue time of instances.
    """
    
    pass


# TODO
class TotalTokenNumberOfQueuePolicy(LoadBalancingPolicy):
    """Total Token Number Of Queue load balancing policy.
    This policy selects the replica based on the total token numbers in the queue.
    """
    
    pass

# TODO
class OutputTokenNumPredictionPolicy(LoadBalancingPolicy):
    """Input String Length Aware load balancing policy.
    This policy selects the replica based on predicted output token numbers.
    S1: TODO
    """
    
    pass

# TODO
class InstanceQueuePolicy(LoadBalancingPolicy):
    """Instance Queue load balancing policy.
    This policy selects the replica based on the queue load of the instance.
    S1: queue length - number of requests in the queue.
    S2: total tokens - total number of tokens in the queue.
    S3: Waiting time - total waiting time of all requests in the queue.
    S4: TODO
    """

    pass