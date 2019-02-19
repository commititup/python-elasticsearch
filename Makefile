all: *

INSTALL_INIT = install

.PHONY: all

install:
	mkdir -p $(DESTDIR)/usr/local/lib/python2.7/site-packages/mod_elasticsearch
	mkdir -p $(DESTDIR)/etc/config
	cp -r mod_elasticsearch/*.py $(DESTDIR)/usr/local/lib/python2.7/site-packages/mod_elasticsearch
	cp -r mod_elasticsearch/kibanaConfig.json $(DESTDIR)/etc/config/
	chmod +x $(DESTDIR)/usr/local/lib/python2.7/site-packages/mod_elasticsearch/*.py


