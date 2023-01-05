MODULE_LIST := $(shell find . -type f \( -iname "*.py" ! -iname "*__init__.py" ! -iname "*test_*.py" ! -iname "*config*.py" \))
test:
	@python3 -m unittest discover -v

out_test:
	@python3 -m coverage run -m unittest
	@python3 -m coverage html $(MODULE_LIST) && open htmlcov/index.html 

dashboard:
	docker compose up metabase -d