cwlVersion: v1.0
class: CommandLineTool
inputs:
  inp2:
    type: Directory
    default:
      class: Directory
      basename: inp2
      listing:
        - class: File
          basename: "hello.txt"
          contents: "hello world"
outputs: []
arguments: [echo, $(inputs.inp2)]