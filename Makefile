COMMAND = flake8  bumblebee && py.test -s

tdd:
	watchmedo shell-command --command='clear; $(COMMAND)' --drop

test:
	$(COMMAND)
