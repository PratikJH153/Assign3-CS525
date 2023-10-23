.PHONY: all
all: test_assign1

test_assign1: test_assign3_1.c storage_mgr.c dberror.c buffer_mgr.c buffer_mgr_stat.c replacement_mgr_strat.c expr.c record_mgr.c rm_serializer.c
	gcc -g -o test_assign1 test_assign3_1.c storage_mgr.c dberror.c buffer_mgr.c buffer_mgr_stat.c replacement_mgr_strat.c expr.c record_mgr.c rm_serializer.c

.PHONY: clean
clean:
	rm test_assign1