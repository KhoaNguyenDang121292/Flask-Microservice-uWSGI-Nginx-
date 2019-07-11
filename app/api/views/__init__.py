from api.views.base.v1.routes import base_v1
from flask import render_template


def page_not_found(e):
  return render_template('error_404.html'), 404

def initialize(app):
    # Service components registration
    app.register_blueprint(base_v1, url_prefix='/v1')
    app.register_error_handler(404, page_not_found)
