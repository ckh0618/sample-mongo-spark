package com.mongodb.samples.spark;

import java.io.Serializable;

public class GradeVo implements Serializable {
    private Double student_id;
    private Double class_id;

    public Double getStudent_id() {
        return student_id;
    }

    public void setStudent_id(Double student_id) {
        this.student_id = student_id;
    }

    public Double getClass_id() {
        return class_id;
    }

    public void setClass_id(Double class_id) {
        this.class_id = class_id;
    }

    public String toString() {
        return "StudentID: " + this.student_id + " ClassID: " + this.class_id ;
    }
}
