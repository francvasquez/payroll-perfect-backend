## Configure region for Payroll Perfect whic is in us-west-1
aws configure set region us-west-1
## Update function
# zip first
zip -r function.zip .
# upload to lambda
aws lambda update-function-code --function-name analytics-backend --zip-file fileb://function.zip
## Random
# Find all refernces to "xyz"
grep -r "xyz" .

