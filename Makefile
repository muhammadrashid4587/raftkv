.PHONY: test demo clean

test:
	python3 -m unittest discover -s tests -v

demo:
	bash scripts/demo.sh

clean:
	rm -rf /tmp/raftkv-demo
	find . -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true
	find . -name '*.pyc' -delete
