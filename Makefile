prj-dir := $(shell pwd)
src-dir := $(prj-dir)
venv-dir := $(prj-dir)/venv
python-native := python3
python-venv := $(venv-dir)/bin/python
nose := $(venv-dir)/bin/nose2
pip := $(venv-dir)/bin/pip

define get_site_dir
$(shell $(python-venv) -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")
endef

init: create-env create-dirs install-deps set-path

create-env:
	$(python-native) -m venv $(venv-dir)

create-dirs:
	mkdir -p log

install-deps:
	$(pip) install -r requirements.txt

set-path:
	echo $(src-dir) > $(call get_site_dir)/my.pth

run:
	$(python-venv) $(filter-out $@, $(MAKECMDGOALS))

test: init
	$(nose)

clean:
	rm -rf $(venv-dir) $(prj-dir)/build $(prj-dir)/dist ${prj-dir}/maxwell_client.egg-info