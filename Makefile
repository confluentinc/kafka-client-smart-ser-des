#this is important to have as without this all make commands will default to using master as the value
MASTER_BRANCH ?= main
UPDATE_MK_INCLUDE := true
UPDATE_MK_INCLUDE_AUTO_MERGE := true
SERVICE_NAME := csid-smart-ser-des
IMAGE_NAME := $(SERVICE_NAME)

include ./mk-include/cc-begin.mk
include ./mk-include/cc-vault.mk
include ./mk-include/cc-end.mk
