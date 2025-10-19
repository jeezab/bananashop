import pytest
from pyspark.sql import SparkSession, Row
from product_category import product_category_pairs

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("product-category-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()

def test_pairs_and_products_without_categories(spark):
    products_df = spark.createDataFrame(
        [
            Row(id=1, name="Apple"),
            Row(id=2, name="Banana"),
            Row(id=3, name="Desk"),        # без категории
            Row(id=4, name="Cable")        # будет отображаться в отсутствующую категорию
        ]
    )
    categories_df = spark.createDataFrame(
        [
            Row(id=10, name="Fruits"),
            Row(id=11, name="Office"),
        ]
    )
    relations_df = spark.createDataFrame(
        [
            Row(product_id=1, category_id=10),   # Apple -> Fruits
            Row(product_id=2, category_id=10),   # Banana -> Fruits
            Row(product_id=2, category_id=11),   # Banana -> Office
            Row(product_id=2, category_id=10),   # повтор
            Row(product_id=4, category_id=12),   # отсутствующая категория
        ]
    )

    result = product_category_pairs(products_df, categories_df, relations_df)

    got = {(r["product_name"], r["category_name"]) for r in result.collect()}
    expected = {
        ("Apple",  "Fruits"),
        ("Banana", "Fruits"),
        ("Banana", "Office"),
        ("Desk",   None),   # продукт без категории
        ("Cable",  None),   # несуществующая категория
    }
    assert got == expected

def test_result_has_expected_columns(spark):
    products_df = spark.createDataFrame([Row(id=1, name="A")])
    categories_df = spark.createDataFrame([Row(id=1, name="C")])
    relations_df = spark.createDataFrame([Row(product_id=1, category_id=1)])

    df = product_category_pairs(products_df, categories_df, relations_df)

    # Simple schema assertions
    cols = df.columns
    assert cols == ["product_name", "category_name"]
    # Nullability check for category_name (should allow nulls)
    field = [f for f in df.schema.fields if f.name == "category_name"][0]
    assert field.nullable is True
