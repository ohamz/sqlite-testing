class QueueEntry:
    """
    A class representing a queue entry for an SQL query.

    sql (str): The SQL query string.
    mutation_count (int): The number of mutations applied to the query.
    prev_coverage (int): The previous coverage value before the last mutation.
    new_coverage (int): The new coverage value after the last mutation.
    last_technique (str): The last mutation technique used.
    """
    def __init__(self, sql, cov, mutation_count=0):
        self.sql = sql                     
        self.mutation_count = mutation_count
        self.prev_coverage = cov
        self.new_coverage = cov

    def update_coverage(self, new_cov):
        self.new_coverage = new_cov

    def has_new_coverage(self):
        return self.new_coverage > self.prev_coverage
    
    def reset_mutation_count(self):
        self.mutation_count = 0

    def __repr__(self):
        return f"<Query (mut#{self.mutation_count}, cov-old: {self.prev_coverage}, cov-new: {self.new_coverage})>"
