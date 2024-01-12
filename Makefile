VERSION = 1.7.0
NAME = grnoc-tsds-services

rpm: dist
	rpmbuild -ta  dist/$(NAME)-$(VERSION).tar.gz

oel8: dist
	carton install --deployment --path=dist/$(NAME)-$(VERSION)/venv
	cd dist; tar -czvf $(NAME)-$(VERSION).tar.gz $(NAME)-$(VERSION)
	rpmbuild -ta dist/$(NAME)-$(VERSION).tar.gz

el7: dist
	cd dist; tar -czvf $(NAME)-$(VERSION).tar.gz $(NAME)-$(VERSION)
	rpmbuild -ta dist/$(NAME)-$(VERSION).tar.gz

el6: dist
	cd dist; tar -czvf $(NAME)-$(VERSION).tar.gz $(NAME)-$(VERSION)
	rpmbuild -ta dist/$(NAME)-$(VERSION).tar.gz

clean:
	rm -rf dist/$(NAME)-$(VERSION)/
	rm -rf dist

dist: clean
	rm -rf dist/$(NAME)-$(VERSION)/
	mkdir -p dist/$(NAME)-$(VERSIwON)/
	cp -r bin conf init.d lib systemd www CHANGES.md INSTALL.md $(NAME).spec dist/$(NAME)-$(VERSION)/
	cd dist; tar -czvf $(NAME)-$(VERSION).tar.gz $(NAME)-$(VERSION)/

test: 
	/usr/bin/perl t/TEST 1

venv:
	carton install --path=venv