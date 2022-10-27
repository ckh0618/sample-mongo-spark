package com.mongodb.samples.spark;

public class InspectionVo {

    private String id;
    private Long certificate_number;
    private String business_name;
    private String date;
    private String result;
    private AddressVo address;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getCertificate_number() {
        return certificate_number;
    }

    public void setCertificate_number(Long certificate_number) {
        this.certificate_number = certificate_number;
    }

    public String getBusiness_name() {
        return business_name;
    }

    public void setBusiness_name(String business_name) {
        this.business_name = business_name;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public AddressVo getAddress() {
        return address;
    }

    public void setAddress(AddressVo address) {
        this.address = address;
    }
}


