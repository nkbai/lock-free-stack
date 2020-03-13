use std::sync::atomic::Ordering;
use std::sync::atomic::{spin_loop_hint, AtomicPtr};

struct Node {
    value: i32,
    next: *mut Node,
}
struct LockFreeStack {
    next: AtomicPtr<Node>,
}
unsafe impl Sync for LockFreeStack {}
unsafe impl Send for LockFreeStack {}
impl LockFreeStack {
    pub fn new() -> LockFreeStack {
        LockFreeStack {
            next: AtomicPtr::new(std::ptr::null_mut()),
        }
    }
    pub fn push(&self, v: i32) {
        /*
        这里的Ordering应该是什么呢?
        */
        let mut old = self.next.load(std::sync::atomic::Ordering::Relaxed);
        let   new = Box::new(Node {
            value: v,
            next: old,
        });
        let new = Box::into_raw(new);
        loop {
            /*
            这里的Ordering应该是什么呢?
            */
            let prev = self.next.compare_and_swap(old, new, Ordering::AcqRel);
            if prev == old {
                break;
            }
            /*
            毫无疑问,这里是safe的,因为还在new的作用域中,一定没有被释放.
            并且,走到这里说明这个节点还没有加入stack,所以也不存在被pop的可能性.
            */
            unsafe {
                (*new).next = prev;
            }
            old = prev;
            spin_loop_hint();
        }
    }
    pub fn pop(&self) -> Option<i32> {
        let mut old = self.next.load(std::sync::atomic::Ordering::Relaxed);
        loop {
            if old.is_null() {
                return None;
            }
            /*
            关于(*old).next 这个unsafe代码,
            如果cas不成功,那么这里可能取到一个无效的指针,但是因为并不会使用,所以不会出现问题.
            如果cas成功,那么(*old).next一定是安全有效的
            */
            let prev = self
                .next
                .compare_and_swap(old, unsafe { (*old).next }, Ordering::AcqRel);
            if prev == old {
                if prev.is_null() {
                    return None;
                } else {
                    /*
                    这会释放申请的内存,
                    关于unsafe:
                    如果stack并发保护有效,那么拿到的prev指针有定是有效的,其他人也不会拿到.
                    所以不存在二次释放的问题
                    */
                    let prev = unsafe { Box::from_raw(prev) };
                    return Some(prev.value);
                }
            }
            old = prev;
            spin_loop_hint();
        }
    }
}
impl Drop for LockFreeStack {
    /*
     因为&mut self保证了不会有其他人同时操作这个Stack,因此可以放心的一个一个移除即可.
    */
    fn drop(&mut self) {
        let mut next = self.next.load(Ordering::Relaxed);
        while !next.is_null() {
            /*
            因为是独占,所以只要里面的原始数据是有效的,那么这里一定是安全的.
            */
            let n = unsafe { Box::from_raw(next) };
            // println!("drop {}", n.value);
            next = n.next;
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
