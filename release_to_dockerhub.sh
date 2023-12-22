#!/bin/sh

if [ -z "$1" ]; then
    echo "Usage: release.sh TAG"
    return
fi

swpt_trade="epandurski/swpt_trade:$1"
swpt_trade_swagger_ui="epandurski/swpt_trade_swagger_ui:$1"
docker build -t "$swpt_trade" --target app-image .
docker build -t "$swpt_trade_swagger_ui" --target swagger-ui-image .
git tag "v$1"
git push origin "v$1"
docker login
docker push "$swpt_trade"
docker push "$swpt_trade_swagger_ui"
