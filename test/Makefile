PRJ =  $(wildcard *.mak)

all : $(PRJ)
	@for i in $(PRJ); do \
        make -f $$i all;\
    done

clean: $(PRJ)
	@for i in $(PRJ); do \
        make -f $$i clean; \
    done
