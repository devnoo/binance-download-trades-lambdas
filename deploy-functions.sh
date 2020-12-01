export AWS_PROFILE="jobnoo"
export AWS_REGION='us-east-1'
echo  $1
rm -f .serverless/$1.zip
zip  .serverless/$1.zip download_trades_lambdas -r
aws lambda update-function-code --function-name download-trades-dev-$1 --zip-file fileb://.serverless/$1.zip
