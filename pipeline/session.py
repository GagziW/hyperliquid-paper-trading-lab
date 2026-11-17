"""One market-data session: every strategy gets the same events under the same freshness rules.

The live paper trader (``pipeline/paper.py``) and the backtest (``pipeline/backtest.py``) both give
their market events to a ``Session``. The session decides which books and trades are fresh enough,
reports data gaps, and passes each event to one ``PaperEngine`` for each strategy. Because both
programs use this class and the same engine, a backtest applies the same rules as a live run.
"""

CLOCK_SKEW_MS = 250  # a book that is more than this far "in the future" means a wrong clock


class Session:
    def __init__(self, simulation, engines):
        self.simulation, self.engines = simulation, engines
        self.stale = False

    def on_book(self, book, now_ms, waited_ms=0.0):
        """Give a book to the engines, or report one data gap if the book is too old.

        ``waited_ms`` is the time between the receipt of the book and this call.
        """
        limit = self.simulation.max_book_age_ms
        exchange_age_ms = book.received_wall_ms - book.exchange_ms + waited_ms
        if waited_ms <= limit and -CLOCK_SKEW_MS <= exchange_age_ms <= limit:
            self.stale = False
            for engine in self.engines:
                engine.on_book(book)
        elif not self.stale:
            self.stale = True
            self.gap(now_ms, "stale_book_or_clock_skew")

    def on_trade(self, trade, received_ms, book_age_ms):
        """``book_age_ms`` is the age of the newest received book, or None if there is no book."""
        if not self.stale and book_age_ms is not None and book_age_ms <= self.simulation.max_book_age_ms:
            for engine in self.engines:
                engine.on_trade(trade, received_ms)

    def gap(self, now_ms, reason):
        for engine in self.engines:
            engine.gap(now_ms, reason)

    def tick(self, now_ms, book_age_ms):
        """Timers: a feed that went quiet, entry timeouts and holding-time limits."""
        if not self.stale and book_age_ms is not None and book_age_ms > self.simulation.max_book_age_ms:
            self.stale = True
            self.gap(now_ms, "stale_local_book")
        for engine in self.engines:
            engine.tick(now_ms)
