CREATE FUNCTION example_add_two(x DOUBLE) RETURNS DOUBLE RETURN example_add_one(example_add_one(x));
