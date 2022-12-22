build:
	pip install \
	--platform manylinux2014_aarch64 \
	--target=lambda \
	--implementation cp \
	--python 3.9 \
	--only-binary=:all: --upgrade \
	-r requirements.txt
	cp lambda_handler.py lambda
	cd lambda && zip -9 -r ../lambda.zip  *
