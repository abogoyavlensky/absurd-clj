-- Queues used by the cross-SDK check. Python works xsdk_py, Clojure works xsdk_clj.
select absurd.create_queue('xsdk_py');
select absurd.create_queue('xsdk_clj');
