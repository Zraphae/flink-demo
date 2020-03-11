package cn.com.my.common.constant;

public enum OGGOpType {

    INSERT("I"),UPDATE("U"),DELETE("D");

    private String value;

    OGGOpType(String value){
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}