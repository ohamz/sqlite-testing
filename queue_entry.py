class QueueEntry:
    """
    A class representing a queue entry for an SQL query.

    sql (str): The SQL query string.
    mutation_count (int): The number of mutations applied to the query.
    prev_coverage (int): The previous coverage value before the last mutation.
    new_coverage (int): The new coverage value after the last mutation.
    last_technique (str): The last mutation technique used.
    """
    def __init__(self, sql, mutation_count=0, prev_coverage=None, new_coverage=None, last_technique=None):
        self.sql = sql                     
        self.mutation_count = mutation_count
        self.prev_coverage = prev_coverage
        self.new_coverage = new_coverage
        self.last_technique = last_technique

    # TODO: create 2 update functions: one for old coverage and one for new coverage. TO CHECK??
    def update_coverage(self, new_cov):
        self.prev_coverage = self.new_coverage
        self.new_coverage = new_cov

    def has_new_coverage(self):
        if self.prev_coverage is None:
            return False
        return self.new_coverage > self.prev_coverage

    def __repr__(self):
        return f"<Query (mut#{self.mutation_count}, cov {self.prev_coverage}→{self.new_coverage}, technique={self.last_technique})>"
