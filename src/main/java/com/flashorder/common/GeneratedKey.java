package com.flashorder.common;

public final class GeneratedKey {
    private long id;

    public void setId(long id) {
        this.id = id;
    }

    public long requireId() {
        if (id <= 0) {
            throw new IllegalStateException("The insert did not return a generated ID");
        }
        return id;
    }
}
