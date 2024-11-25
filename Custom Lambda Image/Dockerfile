# Use the official AWS Lambda Python 3.9 base image
FROM public.ecr.aws/lambda/python:3.9

# Copy your application code to the container
COPY app.py ${LAMBDA_TASK_ROOT}

# Add the contents of the Lambda Layer (requests)
COPY ../layer/python /opt/python/

# Set the CMD to your handler
CMD ["app.lambda_handler"]
