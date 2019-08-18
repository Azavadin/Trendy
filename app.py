from flask import Flask, request
from flask_restful import Resource, Api

link = Flask(__name__)
link_api = Api(link)

class Link(Resource):
    def get(self):
        return {'hello': 'world'}
    def post(self):
        params = request.get_json(force=True)
        user = params.userid
        url = params.url
        return 'Hit me'

link_api.add_resource(Link, '/links')