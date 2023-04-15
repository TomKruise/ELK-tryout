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

    public Item(Long id, String title, String category, String brand, Double price, String images) {
        this.id = id;
        this.title = title;
        this.category = category;
        this.brand = brand;
        this.price = price;
        this.images = images;
    }
}
