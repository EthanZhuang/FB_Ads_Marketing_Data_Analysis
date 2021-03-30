from gevent.pywsgi import WSGIServer
from api_log import write_log
from flask import Flask, jsonify, request, Response
from UserCF import UserCF
import status_code
import pandas as pd

app = Flask(__name__)
app.config["DEBUG"] = False

train_data = pd.read_pickle('./storage/training_data/train_data.pkl')
usercf = UserCF(train_data)
print('user_cf is loading successfully')


@app.before_request
def before_request():
    write_log('info', "User requests info, path: {0}, method: {1}, ip: {2}, agent: {3}"
              .format(str(request.path), str(request.method), str(request.remote_addr), str(request.user_agent)))


@app.route('/recommendation', methods=['POST'])
def result():
    """
    post_data:{'UserID': XXXX}
    """

    try:
        post_data = request.get_json()

        recommendation: dict = usercf.UserCFRecommend(user=int(post_data['UserID']), k=100, newest_ArticleId=None)
        response = {int(post_data['UserID']): recommendation}
        return jsonify(response)

    except Exception as e:
        write_log('error', e.args[0])
        return status_code.result(400, 'error')


if __name__ == '__main__':

    http_server = WSGIServer(('0.0.0.0', 5003), app)
    http_server.serve_forever()
    app.run(threaded=True)
