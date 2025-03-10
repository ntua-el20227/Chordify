from flask import g
from .delete_routes import delete_bp
from .insert_routes import insert_bp
from .query_routes import query_bp
from .join_depart_routes import join_depart_bp
from .transfer_keys_routes import transfer_keys_bp

def register_routes(app,node):
    @app.before_request
    def before_request():
        g.node = node
        
    app.register_blueprint(delete_bp)
    app.register_blueprint(insert_bp)
    app.register_blueprint(query_bp)
    app.register_blueprint(join_depart_bp)
    app.register_blueprint(transfer_keys_bp)
