//
//  ViewController.swift
//  MMap
//
//  Created by zsq on 2018/4/10.
//  Copyright © 2018年 qianmi. All rights reserved.
//

import UIKit

let MEM_SIZE:UInt64 = 1024 * 1024 * 4
let filePath = "/Users/zsq/Desktop/MMap/MMap/data.txt"
let filePathOut = "/Users/zsq/Desktop/MMap/MMap/dataout.txt"
class ViewController: UIViewController {

    override func viewDidLoad() {
        super.viewDidLoad()
        // Do any additional setup after loading the view, typically from a nib.

       

//        var statInfo : stat = stat()
        
//        let fd = open(filePath, O_RDONLY)
//        debugPrint("\(fd)")
        
//        let fdout = open(filePathOut, O_RDWR)
       
//        // 写大文件
//        let fd = open(filePath, O_RDWR | O_APPEND)
//        let str = "abcdefghijklmnopqrstuvwxyz0123456789"
//        var count = 0
//        for _ in 1...1000000 {
//        let result =  write(fd, str, str.count)
//            if count == 0 {
//               debugPrint("result=\(result)")
//            }
//            count = count + 1
//        }
//        debugPrint("写大文件结束")
//        close(fd)



//        if fd < 0  || fdout < 0 {
//            debugPrint("open failed")
//        } else {
//            if fstat(fd, &statInfo) != 0 {
//                debugPrint("fstat failed")
//            } else {
//
//                //        参数1：如果是0,内核帮找一块内存区域，起始地址是返回值addr
//                //        参数2:虚拟内存的大小，0x1000一页（如果申请的内存空间没有对齐，内核会帮我们对齐，会经过一次if/else判断，浪费开销）
//                //        参数3：权限
//                //        参数4：权限
//                //        参数5：fd，将该文件映射到该区域
//                //        参数6：映射的偏移量
//                //        mmap(<#T##UnsafeMutableRawPointer!#>, <#T##Int#>, <#T##Int32#>, <#T##Int32#>, <#T##Int32#>, <#T##off_t#>)
//                //        mmap(UnsafeMutableRawPointer.init(bitPattern: 0), 4 * 1024, PROT_READ, MAP_SHARED, fd, 0)
//
//
//                let opQueue = OperationQueue.init()
//                opQueue.maxConcurrentOperationCount = 8
//
//                let fileSize = statInfo.st_size;
//
//                let times = Int(fileSize) / MEM_SIZE
//                let leftSize = Int(fileSize) % MEM_SIZE
//
//                FileHandle;
//                for i in 1...times {
//
//                    opQueue.addOperation {
//                       let filePartPtr =   mmap(nil, MEM_SIZE, PROT_READ, MAP_SHARED, fd, off_t(i * MEM_SIZE))
//                        if filePartPtr == MAP_FAILED {
//                            print("map failed i = \(i)")
//                        } else {
//                            var buf  =  malloc(MEM_SIZE)
////                            filePartPtr?.assumingMemoryBound(to: Int.IntegerLiteralType)
//                            let res = read(Int32(), buf, MEM_SIZE)
//                            if res > 0 {
//                                print("read size = \(res)")
//                               let wirteSize = write(fdout, buf, MEM_SIZE)
//                                print("wirteSize = \(wirteSize)")
//
//                            }
//                        }
//                    }
//
//                }
//
////                opQueue.compl
//
//                if leftSize > 0 { //有剩余的需要处理
//
//                }
//            }
//        }

        
//        mmap(UnsafeMutableRawPointer!, <#T##Int#>, <#T##Int32#>, <#T##Int32#>, <#T##Int32#>, <#T##off_t#>)
        
        handleFile()
    }
    
    func handleFile() {
        
        let filePath = Bundle.main.path(forResource: "data", ofType: "txt")
        let filePathOut = Bundle.main.path(forResource: "dataout", ofType: ".txt")
        
        let fhIn = FileHandle.init(forReadingAtPath: filePath!)
        let fhOut = FileHandle.init(forWritingAtPath: filePathOut!)
        
        // 清空输出文件的内容
        fhOut?.truncateFile(atOffset: 0)
        
        // 获取文件大小
        let fileSize = fhIn?.seekToEndOfFile()
        
        // 由于要处理的文件较大，所以不能一次将文件全部映射进内存，采用一个合适的映射内存大小。
        // 通过count来执行映射次数
        let count = fileSize! / MEM_SIZE
        // 剩余部分内存
        let leftSize = fileSize! % MEM_SIZE
        
        
        // 两部分分开处理，剩余部分写入
        // 文件最后的，写入文件的最开始位置
        let leftPart = mmap(UnsafeMutableRawPointer.init(bitPattern: 0), Int(leftSize), PROT_READ, MAP_SHARED, fhIn!.fileDescriptor, off_t(MEM_SIZE * count))
        if leftPart == MAP_FAILED {
            print("剩余部分映射失败)")
            return
        }
        let leftBuf = malloc(Int(leftSize))
        memcpy(leftBuf, leftPart, Int(leftSize))
        var data = Data.init(bytes: leftBuf!, count: Int(leftSize))
        data.reverse()
        fhOut?.write(data)
        fhOut?.synchronizeFile()
        
        munmap(leftPart, Int(leftSize))
        free(leftBuf)
        
        print("剩余部分写入成功")
        
        
        // 多线程处理大小的内存，加快处理速度
        let queue = OperationQueue.init()
        queue.maxConcurrentOperationCount = 5 // 设置最大并发数，线程太多，因为线程切换，速度反而也降低

        // 使用信号量，防止资源写入的时候，多线程seek文件的问题
        let semaphore = DispatchSemaphore.init(value: 1)
        
        // 从第0到count段数据，分别写入文件的相应位置
        for i in 0..<count {
            
            queue.addOperation {
                let part = mmap(UnsafeMutableRawPointer.init(bitPattern: 0), Int(MEM_SIZE), PROT_READ, MAP_SHARED, fhIn!.fileDescriptor, off_t(i * MEM_SIZE))
                
                if part == MAP_FAILED {
                    print("映射失败 i = \(i)")
                    return
                }
                let buf = malloc(Int(MEM_SIZE))
                memcpy(buf, part, Int(MEM_SIZE))
                var data = Data.init(bytes: buf!, count: Int(MEM_SIZE))
                // 数据倒叙
                data.reverse()
                free(buf)
                
                semaphore.wait() // 抢占信号资源
                fhOut?.seek(toFileOffset: leftSize + MEM_SIZE * (count - i - 1))
                fhOut?.write(data)
                fhOut?.synchronizeFile()
                semaphore.signal() // 释放信号资源

                munmap(part, Int(MEM_SIZE))
                print("操作成功 i= \(i)")
            }
        }
        
        //等队列中所有操作结束，才能执行后面的close句柄的操作
        queue.waitUntilAllOperationsAreFinished()
        fhIn?.closeFile()
        fhOut?.closeFile()
    }

    override func didReceiveMemoryWarning() {
        super.didReceiveMemoryWarning()
        // Dispose of any resources that can be recreated.
    }


}

