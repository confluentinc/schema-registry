# Dependencies you'll probably need to install to compile this: make, curl, git,
# zip, unzip, patch, java7-jdk | openjdk-7-jdk, maven.

SHELL=/bin/bash

ifndef VERSION
# Note that this is sensitive to this package's version being the first
# <version> tag in the pom.xml
VERSION=$(shell grep version pom.xml | head -n 1 | awk -F'>|<' '{ print $$3 }')
endif

ifndef SECURITY_SUFFIX
SECURITY_SUFFIX=
endif

export PACKAGE_TITLE=schema-registry
export FULL_PACKAGE_TITLE=confluent-schema-registry
export PACKAGE_NAME=$(FULL_PACKAGE_TITLE)-$(VERSION)$(SECURITY_SUFFIX)

# Defaults that are likely to vary by platform. These are cleanly separated so
# it should be easy to maintain altered values on platform-specific branches
# when the values aren't overridden by the script invoking the Makefile

# Whether we should apply patches. This only makes sense for alternate packaging
# systems that know how to apply patches themselves, e.g. Debian.
APPLY_PATCHES?=yes

# DESTDIR may be overriden by e.g. debian packaging
ifeq ($(DESTDIR),)
DESTDIR=$(CURDIR)/BUILD/
endif

ifeq ($(PACKAGE_TYPE),archive)
PREFIX=$(PACKAGE_NAME)
SYSCONFDIR=$(PREFIX)/etc/$(PACKAGE_TITLE)
SYSTEMDDIR=$(PREFIX)/lib/systemd/system
else
PREFIX=/usr
SYSCONFDIR=/etc/$(PACKAGE_TITLE)
SYSTEMDDIR=/lib/systemd/system
endif

# Whether we should run tests during the build.
SKIP_TESTS?=no

all: install


archive: install
	rm -f $(CURDIR)/$(PACKAGE_NAME).tar.gz && cd $(DESTDIR) && tar -czf $(CURDIR)/$(PACKAGE_NAME).tar.gz $(PREFIX)
	rm -f $(CURDIR)/$(PACKAGE_NAME).zip && cd $(DESTDIR) && zip -r $(CURDIR)/$(PACKAGE_NAME).zip $(PREFIX)

apply-patches: $(wildcard debian/patches/*)
ifeq ($(APPLY_PATCHES),yes)
	git reset --hard HEAD
	cat debian/patches/series | xargs -iPATCH bash -c 'patch -p1 < debian/patches/PATCH'
endif

build: apply-patches
ifeq ($(SKIP_TESTS),yes)
	mvn -B -DskipTests=true install
else
	mvn -B install
endif

BINPATH=$(PREFIX)/bin
LIBPATH=$(PREFIX)/share/java/$(PACKAGE_TITLE)
DOCPATH=$(PREFIX)/share/doc/$(PACKAGE_TITLE)

INSTALL=install -D -m 644
INSTALL_X=install -D -m 755

install: build
	rm -rf $(DESTDIR)$(PREFIX)
	mkdir -p $(DESTDIR)$(PREFIX)
	mkdir -p $(DESTDIR)$(BINPATH)
	mkdir -p $(DESTDIR)$(LIBPATH)
	mkdir -p $(DESTDIR)$(SYSCONFDIR)
	for svc in debian/*.service ; do \
	    $(INSTALL) -o root -g root -D $$svc $(DESTDIR)$(SYSTEMDDIR)/$$(basename $$svc) ; \
	done
	pushd "package-schema-registry/target/kafka-schema-registry-package-$(VERSION)$(SECURITY_SUFFIX)-package" ; \
	find bin/ -type f | grep -v README[.]rpm | xargs -I XXX $(INSTALL_X) -o root -g root XXX $(DESTDIR)$(PREFIX)/XXX ;\
	find share/ -type f | grep -v README[.]rpm | xargs -I XXX $(INSTALL) -o root -g root XXX $(DESTDIR)$(PREFIX)/XXX ; \
	pushd etc/schema-registry/ ; \
	find . -type f | grep -v README[.]rpm | xargs -I XXX $(INSTALL) -o root -g root XXX $(DESTDIR)$(SYSCONFDIR)/XXX

	# kafka-serde-tools
	pushd "package-kafka-serde-tools/target/kafka-serde-tools-package-$(VERSION)$(SECURITY_SUFFIX)-package"; \
	find bin/ -type f | grep -v README[.]rpm | xargs -I XXX $(INSTALL_X) -o root -g root XXX $(DESTDIR)$(PREFIX)/XXX ; \
	find share/ -type f | grep -v README[.]rpm | xargs -I XXX $(INSTALL) -o root -g root XXX $(DESTDIR)$(PREFIX)/XXX ; \
	popd


clean:
	rm -rf $(BUILDDIR)
	rm -rf $(CURDIR)/$(PACKAGE_NAME)*
	rm -rf $(FULL_PACKAGE_TITLE)-$(RPM_VERSION)*rpm
	rm -rf RPM_BUILDING

distclean: clean
	git reset --hard HEAD
	git status --ignored --porcelain | cut -d ' ' -f 2 | xargs rm -rf

test:

.PHONY: clean install



RPM_VERSION=$(shell echo $(VERSION) | sed -e 's/-alpha[0-9]*//' -e 's/-beta[0-9]*//' -e 's/-rc[0-9]*//' -e 's/-SNAPSHOT//' -e 's/-cp[0-9]*//' -e 's/-hotfix[0-9]*//' -e 's/-[0-9]*//')
# Get any -alpha, -beta (preview), -rc (release candidate), -SNAPSHOT (nightly), -cp (confluent patch), -hotfix piece that we need to put into the Release part of
# the version since RPM versions don't support non-numeric
# characters. Ultimately, for something like 0.8.2-beta, we want to end up with
# Version=0.8.2 Release=0.X.beta
# where X is the RPM release # of 0.8.2-beta (the prefix 0. forces this to be
# considered earlier than any 0.8.2 final releases since those will start with
# Version=0.8.2 Release=1)
RPM_RELEASE_POSTFIX=$(subst -,,$(subst $(RPM_VERSION),,$(VERSION)))
ifneq ($(RPM_RELEASE_POSTFIX),)
	RPM_RELEASE_POSTFIX_UNDERSCORE=_$(RPM_RELEASE_POSTFIX)
	RPM_RELEASE_ID=0.$(REVISION).$(RPM_RELEASE_POSTFIX)
else
	RPM_RELEASE_ID=$(REVISION)
endif



rpm: RPM_BUILDING/SOURCES/$(FULL_PACKAGE_TITLE)-$(RPM_VERSION).tar.gz
	echo "Building the rpm"
	rpmbuild --define="_topdir `pwd`/RPM_BUILDING" -tb $<
	find RPM_BUILDING/{,S}RPMS/ -type f | xargs -n1 -iXXX mv XXX .
	echo
	echo "================================================="
	echo "The rpms have been created and can be found here:"
	@ls -laF $(FULL_PACKAGE_TITLE)*rpm
	echo "================================================="

# Unfortunately, because of version naming issues and the way rpmbuild expects
# the paths in the tar file to be named, we need to rearchive the package. So
# instead of depending on archive, this target just uses the unarchived,
# installed version to generate a new archive. Note that we always regenerate
# the symlink because the RPM_VERSION doesn't include all the version info -- it
# can leave of things like -beta, -rc1, etc.
RPM_BUILDING/SOURCES/$(FULL_PACKAGE_TITLE)-$(RPM_VERSION).tar.gz: rpm-build-area install debian/$(FULL_PACKAGE_TITLE).spec.in RELEASE_$(RPM_VERSION)$(RPM_RELEASE_POSTFIX_UNDERSCORE)
	rm -rf $(FULL_PACKAGE_TITLE)-$(RPM_VERSION)
	mkdir $(FULL_PACKAGE_TITLE)-$(RPM_VERSION)
	cp -R $(DESTDIR)/* $(FULL_PACKAGE_TITLE)-$(RPM_VERSION)
	sed "s@##RPMVERSION##@$(RPM_VERSION)@g; s@##RPMRELEASE##@$(RPM_RELEASE_ID)@g" <  debian/$(FULL_PACKAGE_TITLE).spec.in > $(FULL_PACKAGE_TITLE)-$(RPM_VERSION)/$(FULL_PACKAGE_TITLE).spec 
	rm -f $@ && tar -czf $@ $(FULL_PACKAGE_TITLE)-$(RPM_VERSION)
	rm -rf $(FULL_PACKAGE_TITLE)-$(RPM_VERSION)

rpm-build-area: RPM_BUILDING/BUILD RPM_BUILDING/RPMS RPM_BUILDING/SOURCES RPM_BUILDING/SPECS RPM_BUILDING/SRPMS

RPM_BUILDING/%:
	mkdir -p $@

RELEASE_%:
	echo 0 > $@

check:
