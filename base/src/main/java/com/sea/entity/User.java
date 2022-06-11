package com.sea.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Sea
 */
@Data
@AllArgsConstructor
public class User {
    private Long id;
    private String name;
    private Integer age;
}
