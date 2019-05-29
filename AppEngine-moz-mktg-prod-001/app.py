# [Start App]
import os
import snippetPerformanceLoadJob
import snippetsMetaDataLoadJob
import snippetsTelemetryDataLoadJob
import flask
import dailyDesktopTelemetryRetrieveJob
import siteMetricsSummaryTable
import siteMetricsByLandingPageSummaryTable
import siteMetricsByExperiment

# [START Config]
app = flask.Flask(__name__)

# [END Config]

@app.route('/')
def welcome():
    return 'App engine operational'

@app.route('/snippetPerformance')
def snippet_performance_load():
    return snippetPerformanceLoadJob.run_snippets_performance_update()

@app.route('/snippetsMetaData')
def snippet_metadata_load():
    return snippetsMetaDataLoadJob.run_snippets_metadata_load_job()

@app.route('/snippetsTelemetryPull')
def snippet_telemetry_load():
    return snippetsTelemetryDataLoadJob.run_snippets_telemetry_update()

@app.route('/desktopCorporateMetrics')
def daily_desktop_corp_metrics_load():
    return dailyDesktopTelemetryRetrieveJob.run_desktop_telemetry_retrieve()

@app.route('/siteMetricsSummaryTable')
def site_metrics_summary_table():
    return siteMetricsSummaryTable.run_site_metrics_update()

@app.route('/siteMetricsByLandingPageSummaryTable')
def site_metrics_by_landing_page_summary_table():
    return siteMetricsByLandingPageSummaryTable.run_site_metrics_landing_page_update()

@app.route('/siteMetricsByExperiment')
def site_metrics_by_experiment():
    return siteMetricsByExperiment.run_site_metrics_by_experiment_update()

if __name__ == '__main__':
    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
# [END app]
