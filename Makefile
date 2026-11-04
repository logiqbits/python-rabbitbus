.PHONY: build publish release test

build:          ## Build sdist + wheel into dist/
	rm -rf dist
	poetry build

publish:        ## Upload dist/ to PyPI (requires `poetry config pypi-token.pypi`)
	poetry publish --no-interaction

release: build publish   ## One command: clean build + publish to PyPI

test:           ## Run the hardening suite against the local broker
	PYTHONPATH=. python tests/test_hardening.py
