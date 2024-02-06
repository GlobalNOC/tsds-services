VERSION = 1.7.1
NAME = grnoc-tsds-services

rpm: dist
	cd dist; tar -czvf $(NAME)-$(VERSION).tar.gz $(NAME)-$(VERSION)
	rpmbuild -ta  dist/$(NAME)-$(VERSION).tar.gz

clean:
	rm -rf dist/$(NAME)-$(VERSION)/
	rm -rf dist

dist: clean venv
	rm -rf dist/$(NAME)-$(VERSION)/
	mkdir -p dist/$(NAME)-$(VERSION)/
	cp -rv bin conf lib systemd www init.d CHANGES.md INSTALL.md venv $(NAME).spec dist/$(NAME)-$(VERSION)/
	cd dist; tar -czvf $(NAME)-$(VERSION).tar.gz $(NAME)-$(VERSION)/

test:
	/usr/bin/perl -I lib/ t/TEST 1

venv:
	carton install --path=venv
