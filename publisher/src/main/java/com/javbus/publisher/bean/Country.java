package com.javbus.publisher.bean;


import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
@ToString
public class Country {
    private String name;
    private String capital;
    private Float population;
    private Float area;
}
