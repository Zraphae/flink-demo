package cn.com.my.common.constant;

public enum OGGOpType {

    I("I"), U("U"), D("D");

    private String value;

    OGGOpType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}