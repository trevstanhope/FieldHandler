#!/usr/bin/python
import zmq
import json
import time
ZMQ_SERVER = 'tcp://127.0.0.1:1980'
USER_ID = 'human1'
WORKER_ID = 'worker1'
SAMPLE_TASK1 = { 
                'task_id':'1',
                'description':'Weed Rows 1-12',
                'requires':'cultivator12'
              }

SAMPLE_TASK2 = { 
                'task_id':'2',
                'description':'Weed Rows 13-25',
                'requires':'cultivator12'
              }

SAMPLE_TASK3 = { 
                'task_id':'3',
                'description':'Weed Rows 1-12',
                'requires':'cultivator24'
              }

class Human:
  def __init__(self):
    self.user_id = USER_ID

  def connect(self):
    context = zmq.Context()
    self.socket = context.socket(zmq.REQ)
    self.socket.connect(ZMQ_SERVER)
    print('[Connecting to TaskManager]...')

  def new_task(self, task):
    print('[Adding a New Task]...')
    message = json.dumps({
                          'type':'new_task',
                          'user_id':self.user_id,
                          'task':task
                        })
    self.socket.send(message)
    response = self.socket.recv()
    print('--> received response...%s' % response)

  def change_task(self, task, worker):
    print('[Changing a Task]...')
    message = json.dumps({
                          'type':'change_task',
                          'user_id':self.user_id,
                          'worker_id':worker,
                          'task':task
                        })
    self.socket.send(message)
    response = self.socket.recv()
    print('--> received response...%s' % response)

  def delete_task(self):
    print('[Deleting a Task]...')
    message = json.dumps({
                          'type':'delete_task',
                          'user_id':self.user_id,
                          'task':SAMPLE_TASK1
                        })
    self.socket.send(message)
    response = self.socket.recv()
    print('--> received response...%s' % response)
  
if __name__ == '__main__':
  human = Human()
  human.connect()
  human.new_task(SAMPLE_TASK1)
  human.change_task(SAMPLE_TASK2,WORKER_ID)
