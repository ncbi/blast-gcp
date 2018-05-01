VERSION := 0.0.1
PACKAGE := blastspecs
PYTHON_PACKAGE := $(PACKAGE)-python-$(VERSION).zip
CPP_PACKAGE := $(PACKAGE)-cpp-$(VERSION).zip
JAVA_PACKAGE := $(PACKAGE)-java-$(VERSION).zip


PROTOS         := $(shell find . -type f -name '*.proto' -printf '%P\n')
PROTOBASE      := $(PROTOS:.proto=) 

TYPES          := cpp python
CLEANTYPES     := $(addprefix clean_, $(TYPES))
EXT_cpp        := .pb.cpp .grpc.pb.cpp .pb.hpp .grpc.pb.hpp 
EXT_python     := _pb2.py _pb2_grpc.py

GENERATED_DIR  := generated
DIR_cpp        := $(GENERATED_DIR)/gen_cpp
DIR_python     := $(GENERATED_DIR)/python

GS_ARTIFACT_PATH := "gs://ncbi-build-artifacts"


define OUTFILES
$(addprefix $(DIR_$(1))/,$(foreach ext, $(EXT_$(1)),$(addsuffix $(ext),$(PROTOBASE))))
endef
#$(addprefix gen_cpp/,$(foreach ext, $(EXT_cpp),$(addsuffix $(ext),$(PROTOBASE))))

# generic mechanism for calling protoc
GRPCDIR        := $(NCBI)/grpc-1.3.1-ncbi1/Release64MT/bin
PROTOC         := $(GRPCDIR)/protoc


all: cpp python

clean: clean_cpp clean_python

clean_cpp:
	rm -rf $(DIR_cpp)

.PHONY: all clean clean_cpp clean_python

$(DIR_cpp)/%.pb.cc $(DIR_cpp)/%.grpc.pb.cc: %.proto 
	$(PROTOC) --cpp_out=$(DIR_cpp) --grpc_out=$(DIR_cpp) -I. --plugin=protoc-gen-grpc=$(GRPCDIR)/grpc_cpp_plugin $<

$(DIR_cpp)/%.pb.cpp: $(DIR_cpp)/%.pb.cc
	mv $< $@

# Build all cpp files needed for grpc
cpp: cpp_dir $(filter %.cpp,$(call OUTFILES,cpp))

# Build all python files needed for grpc
python: python_dir $(call OUTFILES,python)

$(DIR_python)/%_pb2.py $(DIR_python)/%_pb2_grpc.py: %.proto
	$(PROTOC) --python_out=$(DIR_python) --grpc_out=$(DIR_python) -I. --plugin=protoc-gen-grpc=$(GRPCDIR)/grpc_python_plugin $<
	touch $(@D)/__init__.py

$(GENERATED_DIR)/$(PYTHON_PACKAGE):
	(cd $(DIR_python); zip -r9 ../$(PYTHON_PACKAGE) .)

$(GENERATED_DIR)/$(CPP_PACKAGE):
	(cd $(DIR_cpp); zip -r9 ../$(CPP_PACKAGE) .)

packages: $(GENERATED_DIR)/$(CPP_PACKAGE) $(GENERATED_DIR)/$(PYTHON_PACKAGE)

all: dirs blast_request formatter packages


dirs: cpp_dir python_dir

cpp_dir:
	@mkdir -p $(DIR_cpp)

python_dir:
	@mkdir -p $(DIR_python) 

clean_python:
	rm -rf $(call OUTFILES,python)


push: $(GENERATED_DIR)/*.zip
	gsutil -m cp $(GENERATED_DIR)/*.zip $(GS_ARTIFACT_PATH)

