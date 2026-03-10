CC := gcc
CFLAGS ?= -std=c11 -O3 -Wall -Wextra -pedantic
LDFLAGS ?=
LDLIBS ?= -pthread

TARGET := mem_bw_latency
SRC := mem_bw_latency.c

.PHONY: all clean run run-stream-only

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(SRC) $(LDFLAGS) $(LDLIBS) -o $(TARGET)

run: $(TARGET)
	./$(TARGET)

run-stream-only: $(TARGET)
	./$(TARGET) --stream-only -i 100

clean:
	rm -f $(TARGET)
