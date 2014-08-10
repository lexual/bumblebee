COMMAND = flake8 . && py.test -s

tdd:
	watchmedo shell-command --command='clear; $(COMMAND)' --drop

test:
	$(COMMAND)
