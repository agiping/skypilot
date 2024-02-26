"""LoadBalancer: redirect any incoming request to an endpoint replica."""
import logging
import threading
import time

import fastapi
import requests
import uvicorn
import httpx

from sky import sky_logging
from sky.serve import constants
from sky.serve import load_balancing_policies as lb_policies
from sky.serve import serve_utils

logger = sky_logging.init_logger(__name__)


class SkyServeLoadBalancer:
    """SkyServeLoadBalancer: proxy incoming traffic.

    This class accepts any traffic to the controller and proxies it
    to the appropriate endpoint replica according to the load balancing
    policy.

    NOTE: The original version of SkyServe applies HTTP-redirection, 
    in this version, SkyServe proxies the request instead of redirecting it.
    """

    def __init__(self, controller_url: str, load_balancer_port: int) -> None:
        """Initialize the load balancer.

        Args:
            controller_url: The URL of the controller.
            load_balancer_port: The port where the load balancer listens to.
        """
        self._app = fastapi.FastAPI()
        self._controller_url = controller_url
        self._load_balancer_port = load_balancer_port
        self._load_balancing_policy: lb_policies.LoadBalancingPolicy = (
            lb_policies.RoundRobinPolicy())
        self._request_aggregator: serve_utils.RequestsAggregator = (
            serve_utils.RequestTimestamp())

    def _sync_with_controller(self):
        """Sync with controller periodically.

        Every `constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS` seconds, the
        load balancer will sync with the controller to get the latest
        information about available replicas; also, it report the request
        information to the controller, so that the controller can make
        autoscaling decisions.
        """
        # Sleep for a while to wait the controller bootstrap.
        time.sleep(5)

        while True:
            with requests.Session() as session:
                try:
                    # Send request information
                    response = session.post(
                        self._controller_url + '/controller/load_balancer_sync',
                        json={
                            'request_aggregator':
                                self._request_aggregator.to_dict()
                        },
                        timeout=5)
                    # Clean up after reporting request information to avoid OOM.
                    self._request_aggregator.clear()
                    response.raise_for_status()
                    ready_replica_urls = response.json().get(
                        'ready_replica_urls')
                except requests.RequestException as e:
                    print(f'An error occurred: {e}')
                else:
                    logger.info(f'Available Replica URLs: {ready_replica_urls}')
                    self._load_balancing_policy.set_ready_replicas(
                        ready_replica_urls)
            time.sleep(constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS)

    async def _proxy_request(self, request: fastapi.Request, url: str) -> fastapi.responses.Response:
            """Proxy the incoming request to the selected service replica.

            Args:
                request: The incoming request.
                url: The URL of the selected service replica.

            Returns:
                The response from the service replica.
            """
            method = request.method
            headers = {key: value for key, value in request.headers.items()}
            body = await request.body()
            # TODO (Ping Zhang) We may consider reusing the same httpx.AsyncClient for better performance.
            # In the reuse case, we should manually close the client after the service is down.
            async with httpx.AsyncClient() as client:
                resp = await client.request(method, url, headers=headers, content=body)
                return resp

    async def _handle_request(self, request: fastapi.Request):
        """Handle incoming requests by proxying them to service replicas."""
        self._request_aggregator.add(request)
        ready_replica_url = self._load_balancing_policy.select_replica(request)

        if ready_replica_url is None:
            raise fastapi.HTTPException(status_code=503,
                                        detail='No ready replicas. '
                                        'Use "sky serve status [SERVICE_NAME]" '
                                        'to check the replica status.')

        # Construct the full URL to which the request will be proxied
        path = request.url.path
        query_string = request.url.query
        full_url = f'{ready_replica_url}{path}'
        if query_string:
            full_url += f'?{query_string}'

        logger.info(f'Proxying request to {full_url}')
        response = await self._proxy_request(request, full_url)

        return httpx.Response(
            status_code=response.status_code,
            content=response.content,
            headers=response.headers
        )

    def run(self):
        self._app.add_api_route('/{path:path}',
                                self._handle_request,
                                methods=['GET', 'POST', 'PUT', 'DELETE'])

        @self._app.on_event('startup')
        def configure_logger():
            uvicorn_access_logger = logging.getLogger('uvicorn.access')
            for handler in uvicorn_access_logger.handlers:
                handler.setFormatter(sky_logging.FORMATTER)

        threading.Thread(target=self._sync_with_controller, daemon=True).start()

        logger.info('SkyServe Load Balancer started on '
                    f'http://0.0.0.0:{self._load_balancer_port}')

        uvicorn.run(self._app, host='0.0.0.0', port=self._load_balancer_port)


def run_load_balancer(controller_addr: str, load_balancer_port: int):
    load_balancer = SkyServeLoadBalancer(controller_url=controller_addr,
                                         load_balancer_port=load_balancer_port)
    load_balancer.run()
