# Check and merge the latest code from SkyPilot ORG

# git remote -v
# upstream: https://github.com/skypilot-org/skypilot.git
# origin：https://github.com/agiping/skypilot.git

#Navigate to the root of the project (inferred from git)
cd "$(git rev-parse --show-toplevel)"

git checkout serve_k8s_playground
git pull upstream serve_k8s_playground
git push origin serve_k8s_playground

git checkout serve_k8s_playground_ping
git merge serve_k8s_playground
git push origin serve_k8s_playground_ping

git checkout serve_k8s_playground_ha
git pull upstream serve_k8s_playground_ha
git push origin serve_k8s_playground_ha

git checkout serve_k8s_playground_ping_ha
git merge serve_k8s_playground_ha
git push origin serve_k8s_playground_ping_ha