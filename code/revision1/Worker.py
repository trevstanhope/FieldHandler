#!/usr/bin/python
import zmq
import json
import time
ZMQ_SERVER = 'tcp://127.0.0.1:1980'
ID = 'worker1'
EQUIPMENT = ['cultivator12']

class Worker:
  def __init__(self):
    self.id = ID
    self.task = None
    self.equipment = EQUIPMENT

  def connect(self):
    context = zmq.Context()
    self.socket = context.socket(zmq.REQ)
    self.socket.connect(ZMQ_SERVER)
    print('[Connecting to TaskManager]...')

  def alive(self):
    print('[Alive]...')
    message = json.dumps({  
                          'type':'alive',
                          'id':self.id,
                          'equipment':self.equipment
                        })
    self.socket.send(message)
    response = self.socket.recv()
    print('--> received response...%s' % response)

  def request_task(self):
    print('[Requesting Task]...')
    message = json.dumps({
                          'type':'request_task',
                          'id':self.id,
                        })
    self.socket.send(message)
    response = json.loads(self.socket.recv())
    self.task = response['task']
    print('--> Received response: %s' % response)

  def executing_task(self):
    print('[Executing Task]...')
    message = json.dumps({
                          'type':'executing_task',
                          'id':self.id,
                          'task':self.task
                        })
    self.socket.send(message)
    response = json.loads(self.socket.recv())
    print('--> Received response: %s' % response)
    if (response['type'] == 'change_task'):
      self.task = response['task']
      print('--> Changing to task: %s' % response)
    else:
      print('--> Continuing with task')

  def completed_task(self):
    print('[Completed Task]...')
    message = json.dumps({
                          'type':'completed_task',
                          'id':self.id,
                          'task':self.task
                        })
    self.socket.send(message)
    response = json.loads(self.socket.recv())
    print('--> Received response: %s' % response)
  
if __name__ == '__main__':
  robot = Worker()
  robot.connect()
  robot.alive()
  while True:
    print('--------------------')
    robot.request_task()
    robot.executing_task()
    robot.executing_task()
    robot.executing_task()
    robot.completed_task()
