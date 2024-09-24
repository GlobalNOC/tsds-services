VERSION = 1.7.2
NAME = grnoc-tsds-services

rpm: dist
	cd dist; tar -czvf $(NAME)-$(VERSION).tar.gz $(NAME)-$(VERSION)
	rpmbuild -ta  dist/$(NAME)-$(VERSION).tar.gz

clean:
	rm -rf dist/$(NAME)-$(VERSION)/
	rm -rf dist
	rm -rf venv

dist: clean venv
	rm -rf dist/$(NAME)-$(VERSION)/
	mkdir -p dist/$(NAME)-$(VERSION)/
	cp -rv bin conf lib systemd www init.d CHANGES.md INSTALL.md venv $(NAME).spec dist/$(NAME)-$(VERSION)/
	cd dist; tar -czvf $(NAME)-$(VERSION).tar.gz $(NAME)-$(VERSION)/

test: venv
	/usr/bin/perl -I lib/ -I venv/lib/perl5 t/TEST 1

venv:
	carton install --deployment --path=venv

ol8:
	rm -f *.rpm
	docker build -t "containers.github.grnoc.iu.edu/ndca/tsds-services:$(VERSION)" .
	docker run --entrypoint /bin/sleep --name tsds-services-rpm --rm -d "containers.github.grnoc.iu.edu/ndca/tsds-services:$(VERSION)" 3
	docker cp tsds-services-rpm:/root/grnoc-tsds-services-$(VERSION)-1.el8.x86_64.rpm .
