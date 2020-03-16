use std::sync::atomic::Ordering;
extern crate crossbeam_epoch as epoch;
use epoch::{Atomic, Owned, Shared};
pub struct Node {
    value: i32,
    next: *const Node,
}
pub struct LockFreeStack {
    next: Atomic<Node>,
}
unsafe impl Sync for LockFreeStack {}
unsafe impl Send for LockFreeStack {}
impl LockFreeStack {
    pub fn new() -> LockFreeStack {
        LockFreeStack {
            next: Atomic::null(),
        }
    }
    pub fn push(&self, v: i32) {

        let guard = epoch::pin();

        let   mut new = Owned::new(Node {
            value: v,
            next: std::ptr::null(),
        });
        loop {
            let   old = self.next.load(Ordering::Relaxed,&guard);
            new.next=old.as_raw();
            match  self.next.compare_and_set(old, new, Ordering::Release,&guard){
                Ok(_)=>break,
                Err(e)=>{
                    new=e.new;
                    // spin_loop_hint();
                },
            };
        }
    }
    pub fn pop(&self) -> Option<i32> {
        let guard = epoch::pin();
        loop {
            let   old = self.next.load(std::sync::atomic::Ordering::Acquire,&guard);
            /*
            按照as_ref的文档说明,old的load不能是Relaxed,只要确保old的写的另一方是Release,就是安全的
            我们这里无论是Push还是Pop对于old的set访问用得都是Release,因此是安全的.
            */
            match unsafe{old.as_ref()}{
                None=>return None,
                Some(old2)=>{
                    let next=old2.next;

                    if self.next.compare_and_set(old, Shared::from(next),Ordering::Release,&guard).is_ok(){
                        unsafe {
                            /*
                            按照defer_destroy文档,只要我们保证old不会再其他线程使用就是安全的
                            而我们非常确信,这个old不会被其他线程使用,
                            因为这里是defer_destroy,所以解决了ABA问题 (https://en.wikipedia.org/wiki/ABA_problem)
                            */
                            guard.defer_destroy(old);
                        }
                        return Some(old2.value);
                    }
                    // spin_loop_hint();
                }
            }

        }
    }
}
impl Drop for LockFreeStack {
    /*
     因为&mut self保证了不会有其他人同时操作这个Stack,因此可以放心的一个一个移除即可.
    */
    fn drop(&mut self) {
        let guard = epoch::pin();
        let mut next = self.next.load(Ordering::Relaxed,&guard).as_raw() as *mut Node;
        while !next.is_null() {
            /*
            这里的next确定是通过Owned分配的,所以没有安全问题
            并且drop持有的是mutable stack,只有一个线程可以访问,所以也没有并发问题.
            */
            let n = unsafe { Owned::from_raw(next) };
            // println!("drop {}", n.value);
            next = n.next as *mut  Node ;
        }
    }
}

mod tests {
    use super::*;
    #[test]
    fn test_lock_free_stack() {
        let   s = LockFreeStack::new();
        assert_eq!(s.pop(), None);
        s.push(32);
        s.push(27);
        assert_eq!(s.pop(), Some(27));
        assert_eq!(s.pop(), Some(32));
        assert_eq!(s.pop(), None);
        s.push(77);
        s.push(31);
    }
    #[test]
    fn test_push_then_pop() {
        for _i in 0..100 {
            test_lock_free_stack_push_then_pop();
        }
    }
    #[test]
    fn test_lock_free_stack_push_then_pop() {
        extern crate crossbeam_utils;
        use crossbeam_utils::sync::WaitGroup;
        use std::sync::{Arc, Barrier, Mutex};
        use std::thread;

        let mut v: Vec<i32> = (0..100000).collect();
        let mut start = 0;

        let   wg = WaitGroup::new();
        let barrier = Arc::new(Barrier::new(v.len() / 1000));
        let   stack = Arc::new(LockFreeStack::new());
        println!("start");
        while start < v.len() {
            let wg = wg.clone();
            let stack = stack.clone();
            let barrier = barrier.clone();
            let data = Vec::from(&v[start..start + 1000]);
            std::thread::spawn(move || {
                // println!("thread.before start");
                barrier.wait();
                // println!("thread.start");
                for d in data {
                    stack.push(d);
                }
                drop(wg);
            });
            start += 1000;
        }
        wg.wait();
        println!("inert ...");
        let  r = Arc::new(Mutex::new(Vec::new()));
        let barrier = Arc::new(Barrier::new(10));
        let   wg = WaitGroup::new();
        for _ in 0..10 {
            let stack = stack.clone();
            let r = r.clone();
            let wg = wg.clone();
            let barrier=barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                while let Some(n) = stack.pop() {
                    r.lock().unwrap().push(n);
                }
                drop(wg);
            });
        }
        wg.wait();
        let mut r = r.lock().unwrap();
        assert_eq!(v.as_mut_slice().sort(), r.as_mut_slice().sort());
        println!("ok");
    }

    #[test]
    fn test_push_and_pop(){
        for _i in 0..100 {
            test_lock_free_stack_push_and_pop();
        }
    }
    #[test]
    fn test_lock_free_stack_push_and_pop() {
        extern crate crossbeam_utils;
        use crossbeam_utils::sync::WaitGroup;
        use std::sync::{Arc, Barrier, Mutex};
        use std::thread;
        let num:usize =100*1000;
        let mut v: Vec<i32> = (0..100*num as i32 ).collect();
        let mut start = 0;

        let   wg = WaitGroup::new();
        let barrier = Arc::new(Barrier::new(v.len() / num));
        let   stack = Arc::new(LockFreeStack::new());
        println!("start");
        while start < v.len() {
            let wg = wg.clone();
            let stack = stack.clone();
            let barrier = barrier.clone();
            let data = Vec::from(&v[start..start + num]);
            std::thread::spawn(move || {
                // println!("thread.before start");
                barrier.wait();
                // println!("thread.start");
                for d in data {
                    stack.push(d);
                }
                drop(wg);
            });
            start += num;
        }
        let  r = Arc::new(Mutex::new(Vec::new()));
        let barrier = Arc::new(Barrier::new(10));
        let   wg2 = WaitGroup::new();
        for _ in 0..10 {
            let stack = stack.clone();
            let r = r.clone();
            let wg = wg2.clone();
            let barrier=barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                while let Some(n) = stack.pop() {
                    r.lock().unwrap().push(n);
                }
                drop(wg);
            });
        }
        wg.wait();
        println!("push ...");
        wg2.wait();
        println!("pop complete.");

        let mut r = r.lock().unwrap();
        assert_eq!(v.as_mut_slice().sort(), r.as_mut_slice().sort());
        println!("ok");
    }
}
