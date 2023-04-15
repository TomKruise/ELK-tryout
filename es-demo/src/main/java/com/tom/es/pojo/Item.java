package com.tom.es.pojo;

import lombok.Data;

@Data
public class Item {
    private Long id;
    private String title;
    private String category;
    private String brand;
    private Double price;
    private String images;
}
