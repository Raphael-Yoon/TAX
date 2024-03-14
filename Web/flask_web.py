from flask import Flask, render_template, request

app = Flask(__name__)

@app.route('/')
def index():
    return render_template("test.html")

def main():
    app.run(host='127.0.0.1', debug=False, port=80)

@app.route('/calculate', methods=['POST'])
def result():
    print("Python function called")
    form_data = request.form.to_dict()

    param1 = form_data.get('param1')
    param2 = form_data.get('param2')

    print("Param1 = ", param1)
    print("Param2 = ", param2)

    
    return render_template("test.html")

if __name__ == '__main__':
    main()