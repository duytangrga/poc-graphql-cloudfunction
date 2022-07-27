# poc-graphql-cloudfunction

## Install npm packages

    yarn install --network-concurrency=1

## Run app

Run locally from this repo

    NODE_ENV=local node index.js
    
Deploy to cloudfunction
    
1. Install Google Cloud CLI
    
    https://cloud.google.com/sdk/docs/install
      
2. Running this command

        gcloud functions deploy retailnext-query --entry-point handler --runtime nodejs16 --trigger-http
