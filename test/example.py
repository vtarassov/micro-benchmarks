"""
Example implementation of PerfTestUser for testing purposes.

This module demonstrates how to create a concrete implementation
of the PerfTestUser class with a specific document shape.
"""

from typing import Dict, Any
import random
from datetime import datetime
import sys
import os

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from perf_test_user import PerfTestUser, document_shape, workload, pre_load, post_load
from value_range import IntegerRange, InsertionOrder


class ProductDataWorker(PerfTestUser):
    """
    Example data loader simulating e-commerce product data.

    Demonstrates weighted document shapes:
    - 70% simple products (faster inserts)
    - 20% detailed products (with reviews)
    - 10% complex products (with full metadata)

    Uses IntegerRange for deterministic, reproducible data generation with different insertion orders.
    """

    CATEGORIES = ["Electronics", "Clothing", "Books", "Home", "Sports", "Toys"]
    BRANDS = ["BrandA", "BrandB", "BrandC", "BrandD", "BrandE"]

    # Define value ranges as class attributes
    # NOTE: Each document shape should have its own ValueRange instances
    # Sharing ValueRanges across shapes will cause an error

    # ValueRanges for simple_product (70% of documents)
    simple_category_index = IntegerRange(0, 5, insertion_order=InsertionOrder.ASCENDING)
    simple_price_cents = IntegerRange(999, 99999, insertion_order=InsertionOrder.RANDOM)

    # ValueRanges for detailed_product (20% of documents)
    detailed_category_index = IntegerRange(0, 5, insertion_order=InsertionOrder.ASCENDING)
    detailed_price_cents = IntegerRange(999, 99999, insertion_order=InsertionOrder.RANDOM)
    detailed_quantity = IntegerRange(0, 1000, insertion_order=InsertionOrder.DESCENDING)
    detailed_rating_tenths = IntegerRange(10, 50, insertion_order=InsertionOrder.ASCENDING)

    @pre_load
    def setup_collection(self):
        """
        PRE_LOAD: Set up the collection before data loading.

        Drops the collection if it exists to start fresh.
        """
        print(f"Setting up collection: {self.collection_name}")
        # Drop the collection to start fresh
        self.collection.drop()
        print("Collection dropped and ready for loading")

    @pre_load
    def create_initial_indexes(self):
        """
        PRE_LOAD: Create indexes that benefit bulk inserts.

        Creates indexes on _id (implicit) before loading data.
        Other indexes created after data load for better performance.
        """
        print("Creating initial indexes...")
        # The _id index is created automatically, but we can prepare any other
        # indexes that would benefit from being created before bulk insert
        print("Initial indexes ready")

    @document_shape(weight=70)
    def simple_product(self, ctx=None) -> Dict[str, Any]:
        """
        Generate a simple product document (70% of inserts).

        Uses ValueRange for deterministic data generation:
        - category: deterministic category selection
        - price: random pricing distribution

        Args:
            ctx: Data generation context with document_number

        Returns:
            Dict[str, Any]: Simple product structure with ValueRange fields
        """
        doc_num = ctx.document_number if ctx else 0
        return {
            "_id": f"PROD{doc_num:010d}",
            "name": f"Product {doc_num}",
            "category": self.simple_category_index,  # Will be replaced with actual category index
            "price": self.simple_price_cents,  # Will be replaced with actual price in cents
            "created_at": datetime.utcnow()
        }

    @document_shape(weight=20)
    def detailed_product(self, ctx=None) -> Dict[str, Any]:
        """
        Generate a product with reviews (20% of inserts).

        Uses ValueRange for deterministic data generation:
        - category: deterministic category selection
        - price: random pricing distribution
        - quantity: descending order (starts high, decreases)
        - rating: ascending order (improves over time)

        Args:
            ctx: Data generation context with document_number

        Returns:
            Dict[str, Any]: Product with review data
        """
        doc_num = ctx.document_number if ctx else 0
        return {
            "_id": f"PROD{doc_num:010d}",
            "name": f"Product {doc_num}",
            "description": f"This is product number {doc_num} with various features",
            "category": self.detailed_category_index,
            "brand": random.choice(self.BRANDS),  # Keep brand random for variety
            "price": self.detailed_price_cents,
            "quantity": self.detailed_quantity,
            "rating": self.detailed_rating_tenths,  # Will be converted to decimal (e.g., 35 -> 3.5)
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }

    @document_shape(weight=10)
    def complex_product(self, ctx=None) -> Dict[str, Any]:
        """
        Generate a complex product with full metadata (10% of inserts).

        Args:
            ctx: Data generation context with document_number

        Returns:
            Dict[str, Any]: Complete product structure
        """
        doc_num = ctx.document_number if ctx else 0
        return {
            "_id": f"PROD{doc_num:010d}",
            "name": f"Product {doc_num}",
            "description": f"This is product number {doc_num} with various features",
            "category": random.choice(self.CATEGORIES),
            "brand": random.choice(self.BRANDS),
            "price": round(random.uniform(9.99, 999.99), 2),
            "discount_price": round(random.uniform(9.99, 999.99), 2) if random.random() > 0.5 else None,
            "in_stock": random.choice([True, False]),
            "quantity": random.randint(0, 1000),
            "rating": round(random.uniform(1.0, 5.0), 1),
            "reviews_count": random.randint(0, 1000),
            "attributes": {
                "weight": f"{random.randint(1, 100)} oz",
                "dimensions": f"{random.randint(1,20)}x{random.randint(1,20)}x{random.randint(1,20)} inches",
                "color": random.choice(["Red", "Blue", "Green", "Black", "White"])
            },
            "tags": [f"tag{i}" for i in random.sample(range(1, 51), random.randint(3, 10))],
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }

    @workload(weight=70, name="read_product_by_id")
    def read_product_by_id(self):
        """
        Read a random product by ID (70% of workload operations).

        Simulates a common read operation where users look up products by ID.
        """
        # Get a random product ID from the loaded data
        doc_count = self.ctx.get_document_count() if self.ctx else 1000
        random_id = f"PROD{random.randint(0, doc_count - 1):010d}"

        # Perform the read operation
        result = self.collection.find_one({"_id": random_id})

    @workload(weight=20, name="search_by_category")
    def search_by_category(self):
        """
        Search products by category (20% of workload operations).

        Simulates users browsing products by category.
        """
        category = random.choice(self.CATEGORIES)

        # Perform a find operation with limit
        results = list(self.collection.find({"category": category}).limit(10))

    @workload(weight=5)
    def update_product_stock(self):
        """
        Update stock status for a random product (5% of workload operations).

        Simulates inventory updates. Name defaults to method name: "update_product_stock"
        """
        # Get a random product ID
        doc_count = self.ctx.get_document_count() if self.ctx else 1000
        random_id = f"PROD{random.randint(0, doc_count - 1):010d}"

        # Update stock quantity and availability
        new_quantity = random.randint(0, 1000)
        self.collection.update_one(
            {"_id": random_id},
            {
                "$set": {
                    "quantity": new_quantity,
                    "in_stock": new_quantity > 0,
                    "updated_at": datetime.utcnow()
                }
            }
        )

    @workload(weight=5, name="query_expensive_products")
    def query_expensive_products(self):
        """
        Query expensive products using percentile-based pricing (5% of workload operations).

        Demonstrates using get_percentile() and random_range() for targeted queries.
        Queries products in the top 10% of prices (p90-p100).
        Uses the simple_product price range for queries.
        """
        # Get the p90 price threshold from simple_product range
        p90_price, _ = self.simple_price_cents.get_percentile(90.0)

        # Query products with price >= p90
        list(self.collection.find({"price": {"$gte": p90_price}}).limit(10))

    @workload(weight=5, name="query_mid_range_products")
    def query_mid_range_products(self):
        """
        Query mid-range products using percentile ranges (5% of workload operations).

        Demonstrates using random_range() to query products with pricing
        between p40 and p60 (middle of the price distribution).
        Uses the simple_product price range for queries.
        """
        # Get a random price in the p40-p60 range from simple_product range
        random_price, _ = self.simple_price_cents.random_range(40.0, 60.0)

        # Query products near this price (within 10%)
        price_range = int(random_price * 0.1)
        list(self.collection.find({
            "price": {
                "$gte": random_price - price_range,
                "$lte": random_price + price_range
            }
        }).limit(10))

    @post_load
    def create_query_indexes(self):
        """
        POST_LOAD: Create indexes optimized for query workload.

        Creates indexes after bulk insert for better insert performance.
        """
        print("Creating query indexes after data load...")
        # Create compound index for category queries
        self.collection.create_index([("category", 1), ("price", 1)])
        print("Created index on (category, price)")

        # Create index for brand queries
        self.collection.create_index([("brand", 1)])
        print("Created index on brand")

        # Create index for stock queries
        self.collection.create_index([("in_stock", 1)])
        print("Created index on in_stock")

    @post_load
    def verify_data_load(self):
        """
        POST_LOAD: Verify data was loaded correctly.

        Performs validation and prints statistics about loaded data.
        """
        print("Verifying data load...")
        total_docs = self.collection.count_documents({})
        print(f"Total documents loaded: {total_docs}")

        # Check distribution by category
        print("\nDocument distribution by category:")
        for category in self.CATEGORIES:
            count = self.collection.count_documents({"category": category})
            print(f"  {category}: {count}")

        print("Data load verification complete!")

