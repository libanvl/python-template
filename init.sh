# Install dependencies
pipenv install --dev

# Install pyright type checker
pipenv run nodeenv -p
pipenv run npm install pyright

# Setup pre-commit and pre-push hooks
pipenv run pre-commit install -t pre-commit
pipenv run pre-commit install -t pre-push
