cp -f ./config/serverless_PRD.yml ./serverless.yml
cp -f ./config/config_PRD.json ./config.json
serverless deploy
rm -f ./serverless.yml
rm -f ./config.json
