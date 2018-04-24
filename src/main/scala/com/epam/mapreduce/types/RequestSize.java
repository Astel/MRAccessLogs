package com.epam.mapreduce.types;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class RequestSize implements WritableComparable<RequestSize> {
    private int  count;
    private long size;

    public RequestSize(long size, int count) {
        this.count = count;
        this.size = size;
    }

    public RequestSize() {
    }

    @Override
    public int compareTo(RequestSize o) {
        if (o.size > size) return 1;
        else if (o.size < size) return -1;
        else return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(count);
        dataOutput.writeLong(size);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        count = dataInput.readInt();
        size = dataInput.readLong();
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RequestSize that = (RequestSize) o;
        return count == that.count &&
                size == that.size;
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, size);
    }

    @Override
    public String toString() {
        return size + "," + count;
    }
}
