## -*- makefile -*-

ERL := erl
ERLC := $(ERL)c

INCLUDE_DIRS := ../include
DOC_DIR := ../doc
EBIN_DIRS := 
ERLC_FLAGS := -W $(INCLUDE_DIRS:../%=-I ../%) $(EBIN_DIRS:%=-pa %)

ifndef no_debug_info
  ERLC_FLAGS += +debug_info
endif

ifdef debug
  ERLC_FLAGS += -Ddebug
endif

EBIN_DIR := ../ebin
EMULATOR := beam

ERL_SOURCES := $(wildcard *.erl)
ERL_DOCS := $(wildcard *.erl) $(wildcard *.edoc)
ERL_HEADERS := $(wildcard *.hrl) $(wildcard ../include/*.hrl)
ERL_OBJECTS := $(ERL_SOURCES:%.erl=$(EBIN_DIR)/%.$(EMULATOR))
ERL_OBJECTS_LOCAL := $(ERL_SOURCES:%.erl=./%.$(EMULATOR))
APP_FILES := $(wildcard *.app)
EBIN_FILES = $(ERL_OBJECTS) $(APP_FILES:%.app=../ebin/%.app)
MODULES = $(ERL_SOURCES:%.erl=%)

../ebin/%.app: %.app
	cp $< $@

$(EBIN_DIR)/%.$(EMULATOR): %.erl
	$(ERLC) $(ERLC_FLAGS) -o $(EBIN_DIR) $<

./%.$(EMULATOR): %.erl
	$(ERLC) $(ERLC_FLAGS) -o . $<
