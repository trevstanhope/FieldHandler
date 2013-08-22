#!/usr/bin/python
import zmq
import json
ZMQ_SERVER = 'tcp://127.0.0.1:1980'
ID = 'human1'
SAMPLE_TASK1 = { 
                'id':'1',
                'description':'Weed Rows 1-12',
                'requires':['cultivator12']
              }

SAMPLE_TASK2 = { 
                'id':'2',
                'description':'Weed Rows 13-25',
                'requires':['cultivator12']
              }

SAMPLE_TASK3 = { 
                'id':'3',
                'description':'Weed Rows 1-12',
                'requires':['cultivator24']
              }

class Human:
  def __init__(self):
    self.id = ID

  def connect(self):
    context = zmq.Context()
    self.socket = context.socket(zmq.REQ)
    self.socket.connect(ZMQ_SERVER)
    print('[Connecting to TaskManager]...')

  def new_task(self):
    print('[Adding a New Task]...')
    message = json.dumps({
                          'type':'new_task',
                          'id':self.id,
                          'task':SAMPLE_TASK1
                        })
    self.socket.send(message)
    response = self.socket.recv()
    print('--> received response...%s' % response)

  def change_task(self):
    print('[Changing a Task]...')
    message = json.dumps({
                          'type':'change_task',
                          'id':self.id,
                          'worker':'worker1',
                          'task':SAMPLE_TASK2
                        })
    self.socket.send(message)
    response = self.socket.recv()
    print('--> received response...%s' % response)

  def delete_task(self):
    print('[Deleting a Task]...')
    message = json.dumps({
                          'type':'delete_task',
                          'id':self.id,
                          'task':SAMPLE_TASK1
                        })
    self.socket.send(message)
    response = self.socket.recv()
    print('--> received response...%s' % response)
  
if __name__ == '__main__':
  human = Human()
  human.connect()
  human.new_task()
  human.delete_task()
  human.change_task()
