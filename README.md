# Luigi Scraper

This is a little demo of using Luigi to run a scraping process I threw
together as a prototype for a project that I never needed to use. I'm
uploading it here just to illustrate how you might structure a scraper
in Luigi (although there is doubtless room for improvement).

The best way to think of Luigi at its core is as a `distributed make`
where tasks depends on other tasks and tasks only run if the output
they are supposed to produce doesn't already exist (but some tasks are
wrappers that can run every time). That's how this demo works: each
file scraped from the remote website has a corresponding local file as
output that can be used to see if the file was already scraped. In
turn, every processing step makes its own files, and so on...

This particular project pulled footnotes from pages published in the
Crime in the United States report and created CSV files from them. It
wasn't perfect (right now, it only handles some reports), now was it
complete, but it worked enough to give me a rough idea of what the
collected mass of footnotes looks like.

To run locally, you need to install [luigi](https://github.com/spotify/luigi). Then you can run the task as

``` shell
PYTHONPATH='.' luigi --module agency_footnotes ScrapeAgencies --local-scheduler
```

It'll write a lot of stuff to the output, but as it runs you should
see it save files to the `data` directory locally. To rerun, it should
only try to fetch and process files it did not retrieve the first
time.

To forcibly rerun, you need to delete files from the `data` directory
first. Upstream tasks do not automatically rebuild downstream tasks,
so the only option to redo everything is to kill all the saved files
for now.
