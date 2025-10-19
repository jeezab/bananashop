# -*- coding: utf-8 -*-
from pyspark.sql import DataFrame, functions as F

def product_category_pairs(products: DataFrame,
                           categories: DataFrame,
                           relations: DataFrame) -> DataFrame:
    """
    Возвращает пары (product_name, category_name) и продукты без категорий
    
    - products: [id, name]
    - categories: [id, name]
    - relations: [product_id, category_id]
    - Если продукт без категории, category_name будет null
    """
    p = products.select(
        F.col("id").alias("p_id"),
        F.col("name").alias("product_name")
    )
    r = relations.select(
        F.col("product_id").alias("p_id"),
        F.col("category_id").alias("c_id")
    )
    c = categories.select(
        F.col("id").alias("c_id"),
        F.col("name").alias("category_name")
    )
                             
    result = (
        p.join(r, on="p_id", how="left")
         .join(c, on="c_id", how="left")
         .select("product_name", "category_name")
         .dropDuplicates()
    )
    return result
