;; reppy-channels.lisp -- Channels a'la Reppy's PCML
;;
;; DM/HMSC  01/00
;; DM/RAL   04/16 -- Major update and rewrite to accommodate SMP multiprocessing.
;;                   Corrected the implementation of wrap-abort, guard.
;;                   Major simplifications of event/abort handling
;;                   Unlike PCML, we don't get to automatically GC threads doing noting
;;                   but waiting on a signal that would never arrive. Rather, we have
;;                   to actively discard dead threads. And we do so by issuing an abort
;;                   against all other pending event firings after one of them succeeds.
;;                   If that abort calls DISCARD-CHANNEL, then all orphaned threads
;;                   known to that channel will be reclaimed.
;;
;; DM/RAL  08/16 -- Port to Linux/SBCL
;; DM/RAL  02/17 -- lock-free implementation
;; -------------------------------------------------------------

(defpackage #:reppy-channels
  (:use #:common-lisp #:resource)
  (:nicknames #:rch)
  (:export
   ;; error conditions
   #:no-rendezvous
   #:timeout

   ;; events and rendezvous
   #:spawn
   
   #:channel
   #:make-channel
   #:discard-channel
   #:reset-channel
   #:channel-valid
   
   #:recvevt
   #:sendevt
   #:alwaysevt
   #:abortEvt
   #:nonEvt
   #:timerEvt
   #:errorEvt
   #:timeoutEvt
   #:execEvt
   #:peekEvt
   #:pokeEvt
   #:joinEvt
   #:keyEvt
   #:lineEvt
   #:sexpEvt
   #:rpcEvt

   #:dispatch-serial-evt
   #:dispatch-parallel-evt
   #:wait-for-dispatch-group-evt
   #:wait-for-serial-dispatch-evt
   #:wait-for-parallel-dispatch-evt
   
   #:fail
   #:wrap
   #:wrap-abort
   #:wrap-timeout
   #:wrap-error
   #:guard
   #:choose
   #:choose*

   #:sync
   #:select
   #:select*

   #:send
   #:poke
   #:recv
   #:peek

   #:exec-with-timeout
   #:rpc?
   #:rpc!

   #:safe-peek
   #:safe-poke
   #:safe-recv
   #:safe-send

   ;; other useful items
   #:safe-unwind-protect
   #:once-ref

   #:with-channel
   #:with-channels
   ))

(in-package "REPPY-CHANNELS")

;; ------------------------------------------------------
;; So here we have a system composed of three major subsystems...
;;   1. Threads
;;   2. Intercommunication between threads
;;   3. Composable events representing communication possibilities
;;
;; Threads are generated via SPAWN, and each thread attempting to
;; participate in a potential communication with other threads sets up
;; a tree of communication possibilities represented by an outer level
;; COMM-EVENT object. COM-EVENTS are composable objects. Combinators
;; for event objects can develop a tree of event possibilties.
;;
;; COMM-EVENTS are abstract possibilities for communications that
;; become instantiated with SYNC. Until then they remain abstract
;; composable possibilities. At SYNC, the tree of COMM-EVENTS is
;; reified into a list of lambda closures that represents the
;; rendezvous possibilities at the leaves of the event tree.
;;
;; Each closure represents the actions that may be carried out after a
;; rendezvous succeeds. The winning rendezvous closure will be asked
;; to remove all WITH-ABORT clauses leading back to the top of the
;; tree, and then to wrap its communicated data with the chain of WRAP
;; functions leading back to the top of the tree. All other
;; (non-successful) closures will be asked to perform the WITH-ABORT
;; closures leading back to the top of the tree in their respective
;; branches.
;;
;; In this way we can provide for cleanup on failed rendevouz
;; alternatives.
;;
;; To prevent multiple execution of WRAP-ABORT clauses a WRAP-ABORT
;; event keeps a once-only function pointer to the cleanup code. It
;; can return that function pointer only once, thereafter returning a
;; null value. The successful rendezvous branch can assure that none
;; of its own WRAP-ABORTS will get executed by preemptively reading
;; them out before the failed rendezvous branches have a chance to
;; attempt their own cleanup, some of which might possibly include the
;; same cade.
;;
;; To keep cleanup operations to a bounded cost on the winning thread,
;; any actual cleanup code is launched in its own thread.
;;
;; When a thread decides to instantiate a rendezvous with other
;; threads, a COMM object is produced and owned by that thread.  That
;; COMM object represents a proxy of the thread in all communication
;; activities.
;;
;; Threads communicate with each other via CHANNEL objects. A channel
;; object incorporates reader/writer queues and serves as a passive
;; data object joining two sides of a communcation rendezvous. No
;; thread owns a channel. And channels are shared between threads. But
;; channel integrity needs to be maintained while being operated upon
;; by any thread, through careful elision of interrupts and prevention
;; of the possibility for thread KILL while channel queues are being
;; updated.
;;
;; If a thread wishes to try any of several rendezvous possibilities,
;; its COMM object may end up on several channel reader/writer queues.
;; Channel reader/writer queues are implemented as lock-free.
;;
;; A rendezvous is normally a blocking operation awaiting the
;; simultaneous coincidence of reader and writer. Both reader and
;; writer of a channel could block. A channel represents a possibility
;; of multiple writers and multiple readers.
;;
;; In some cases the blocking of channel writers until readers become
;; active may be too punitive. For those cases we offer the extension
;; of non-blocking channel writes through channel POKE events which
;; always succeed immediately. But there is the possibility that its
;; write data may never be retrieved.
;;
;; To support the use of timers for timeout events, we also offer a
;; negating event combinator called Fail which causes a successful
;; rendezvous to behave as though it failed. In that case all higher
;; WRAP-ABORT handlers will be called, as well as those in other
;; branches of the event tree.
;;
;; With Lisp, we are not CML. Our threads are heavyweight by
;; comparison, and they are not automatically scavenged when their
;; parent thread (no such thing here) gets killed or dies.  The
;; situation is not ideal. So what to do?...
;;
;; [... this is where Custodians might help a bit... ]
;;
;; -----------------------------------------------------------------------
;; LW/SBCL Compatibility

(defmacro atomic-value (atomic)
  `(car ,atomic))

(defmacro atomic-incf (place)
  #+:LISPWORKS `(sys:atomic-fixnum-incf (atomic-value ,place))
  #+:SBCL      (let ((v (gensym)))
                 `(let ((,v (sb-ext:atomic-incf (atomic-value ,place))))
                    (1+ ,v))))

(defmacro atomic-decf (place)
  #+:LISPWORKS `(sys:atomic-fixnum-decf (atomic-value ,place))
  #+:SBCL      (let ((v (gensym)))
                 `(let ((,v (sb-ext:atomic-decf (atomic-value ,place))))
                    (1- ,v))))

;; ====================================================================

(defun make-ref (x)
  #F
  (list x))

(defun car-ref (x)
  #F
  `(:car ,x))

(defun cdr-ref (x)
  #F
  `(:cdr ,x))

(defun ref-value (ref)
  #F
  (optima:ematch ref
    ((list _)          (car (the cons ref)))
    ((list :cdr place) (cdr (the cons place)))
    ((list :car place) (car (the cons place)))
    ))

(defun cas (ref old new)
  #F
  (optima:ematch ref
    ((list _)          (system:compare-and-swap (car (the cons ref)) old new))
    ((list :cdr place) (system:compare-and-swap (cdr (the cons place)) old new))
    ((list :car place) (system:compare-and-swap (car (the cons place)) old new))
    ))
    
;; ------------------
;; CCAS - Conditional CAS

(defstruct ccas-desc
  ref old new cond)

(defun ccas (ref old new cond)
  #F
  (declare (cons ref))
  (labels ((try-ccas (desc)
             (declare (ccas-desc desc))
             (cond ((cas ref old desc)
                    (ccas-help desc))
                   (t  (let ((v (ref-value ref)))
                         (when (ccas-desc-p v)
                           (ccas-help v)
                           (try-ccas desc))))
                   )))
    (try-ccas (make-ccas-desc
               :ref      ref
               :old      old
               :new      new
               :cond     cond))
    ))

(defun ccas-help (desc)
  #F
  (declare (ccas-desc desc))
  (let ((new  (if (eq :undecided (ref-value (ccas-desc-cond desc)))
                  (ccas-desc-new desc)
                (ccas-desc-old desc))))
    (cas (ccas-desc-ref desc) desc new)))

(defun ccas-read (ref)
  #F
  (declare (cons ref))
  (let ((v (ref-value ref)))
    (cond ((ccas-desc-p v)
           (ccas-help v)
           (ccas-read ref))

          (t  v)
          )))

;; ------------------
;; MCAS - Multiple CAS

(defstruct mcas-desc
  triples status)

(defun mcas (triples)
  #F
  ;; triples - a sequence of (ref old new) suitable for CAS
  (mcas-help (make-mcas-desc
              :triples triples
              :status  (make-ref :undecided))))

(defun mcas1 (ref old new)
  #F
  (mcas `((,ref ,old ,new))))

(defun mcas-help (desc)
  #F
  (declare (mcas-desc desc))
  (let ((triples    (mcas-desc-triples desc))
        (status-ref (mcas-desc-status desc)))
    
    (labels ((decide (desired)
               (cas status-ref :undecided desired)
               (let ((success (eq :successful (ref-value status-ref))))
                 (map nil #'(lambda (triple)
                              (apply #'patch-up success triple))
                      triples)
                 success))

             (patch-up (success ref old new)
               (cas ref desc (if success new old)))

             (try-mcas (ref old new)
               (ccas ref old desc status-ref)
               (let ((v (ccas-read ref))) ;; ccas-read makes it faster than when using ref-value (!??)
                 (cond ((and (eq v old)
                             (eq :undecided (ref-value status-ref)))
                        ;; must have changed beneath us when we
                        ;; looked, then changed back again. So try
                        ;; again. (ABA Update?)
                        (try-mcas ref old new))
                       
                       ((eq v desc)
                        ;; we got this one, try the next candidate
                       :break)
                       
                       ((mcas-desc-p v)
                        ;; someone else is trying, help them out, then
                        ;; try again
                        (mcas-help v)
                        (try-mcas ref old new))
                       
                       (t ;; not a descriptor, and not eq old with
                          ;; :undecided, so we must have missed our
                          ;; chance
                        (return-from mcas-help (decide :failed)))
                       ))))

      (map nil #'(lambda (triple)
                   (declare (cons triple))
                   (apply #'try-mcas triple))
           triples)
      (decide :successful)
      )))

(defun mcas-read (ref)
  #F
  (declare (cons ref))
  (let ((v (ccas-read ref)))
    (cond ((mcas-desc-p v)
           (mcas-help v)
           (mcas-read ref))

          (t  v)
          )))

;; ====================================================================

(defstruct (lf-queue
            (:constructor %make-lf-queue))
  refcar
  refcdr)

(defun make-lf-queue ()
  (let ((q (list nil)))
    (%make-lf-queue
     :refcar  (car-ref q)
     :refcdr  (cdr-ref q))
    ))

(defun qcas (q triples)
  #F
  (declare (lf-queue q))
  ;; QCAS - a specially adapted MCAS for our queues
  ;;
  ;; making the (cdr q) the leading part of the MCAS keeps other
  ;; threads from making their way partway through their own MCAS when
  ;; some portion of their changes match ours. A kind of exclusive
  ;; access control...
  ;;
  (let ((excl (lf-queue-refcdr q)))
    (and (mcas (cons `(,excl nil t) triples))
         ;; we succeeded, so reset the (cdr q) flag
         (mcas1 excl t nil))))

(defun lf-queue-contents (q)
  #F
  (declare (lf-queue q))
  ;; we have to use explicit (car-ref q) now that we are
  ;; potentially modifying its cdr. Else, match failure.
  ;;
  (let ((carq  (lf-queue-refcar q)))
    (um:when-let (tl (mcas-read carq))
      (let* ((cdrtl  (cdr-ref tl))
             (hd     (mcas-read cdrtl)))
        (cond ((qcas q `((,carq  ,tl  nil)
                         (,cdrtl ,hd  nil)))
               hd)
              (t
               (lf-queue-contents q))
              )))))

(defun lf-queue-send (q item)
  #F
  (declare (lf-queue q))
  (let* ((carq (lf-queue-refcar q))
         (tl   (mcas-read carq))
         (hd   (and tl (mcas-read (cdr-ref tl))))
         (cell (cons item hd)))
    (cond ((qcas q `((,carq                   ,tl  ,cell)
                     (,(cdr-ref (or tl cell)) ,hd  ,cell)) ))
          (t
           (lf-queue-send q item))
          )))

(defun lf-queue-read (q)
  #F
  (declare (lf-queue q))
  (let ((carq  (lf-queue-refcar q)))
    (um:when-let (tl (mcas-read carq))
      (let* ((cdrtl  (cdr-ref tl))
             (hd     (mcas-read cdrtl))
             (ans    (car hd)))
        (cond ((eq hd tl)
               (cond ((qcas q `((,carq ,tl nil)))
                      ans)
                     (t
                      (lf-queue-read q))
                     ))
              (t
               (let ((nxt (mcas-read (cdr-ref hd))))
                 (cond ((qcas q `((,cdrtl ,hd ,nxt)))
                        ans)
                       (t
                        (lf-queue-read q))
                       )))
              )))))

(defun lf-queue-stuff (q items)
  #F
  (declare (lf-queue q))
  (when items
    (let* ((ilast   (last (the cons items)))
           (cdrlast (cdr-ref ilast))
           (carq    (lf-queue-refcar q)))
      (labels ((qstuff ()
                 (let* ((tl    (mcas-read carq))
                        (xcdr  (mcas-read cdrlast)))
                   (cond (tl
                          (let* ((cdrtl  (cdr-ref tl))
                                 (hd     (mcas-read cdrtl)))
                            (cond ((qcas q `((,cdrtl   ,hd   ,items)
                                             (,cdrlast ,xcdr ,hd))))
                                  (t 
                                   (qstuff))
                                  )))
                       
                         (t
                          (cond ((qcas q `((,carq    nil   ,ilast)
                                           (,cdrlast ,xcdr ,items))))
                                (t
                                 (qstuff))
                                ))
                         ))))
        (qstuff))
      )))

;; -------------------------------
;; Interrupt Control

(defmacro with-interrupts-blocked (&body body)
  #+:LISPWORKS `(mp:with-interrupts-blocked ,@body)
  #+:SBCL      `(sb-sys:without-interrupts ,@body))

(defmacro with-interrupts-enabled (&body body)
  #+:LISPWORKS `(mp:with-interrupts-blocked
                  (mp:current-process-unblock-interrupts)
                  ,@body)
  #+:SBCL      `(sb-sys:allow-with-interrupts
                 ,@body))

(defmacro safe-unwind-protect (form &rest unwind-clauses)
  `(with-interrupts-blocked
    (unwind-protect
        (with-interrupts-enabled
         ,form)
      ,@unwind-clauses)))

(defun join-thread (thread)
  #+:LISPWORKS (mp:process-join thread)
  #+:SBCL      (sb-thread:join-thread thread))

;; ---------------------------------------------
;; Timer Support

(defun make-timer (fn &rest args)
  #+:LISPWORKS (apply #'mp:make-timer fn args)
  #+:SBCL      (sb-ext:make-timer #'(lambda ()
                                      (apply fn args))))

(defun schedule-timer (timer dt &key absolute)
  #+:LISPWORKS
  (if absolute
      (mp:schedule-timer timer dt)
    (mp:schedule-timer-relative timer dt))

  #+:SBCL
  (sb-ext:schedule-timer timer dt :absolute absolute)) ;; fix me!!

(defun unschedule-timer (timer)
  #+:LISPWORKS (mp:unschedule-timer timer)
  #+:SBCL      (sb-ext:unschedule-timer timer))

;; -------------------------------------------------------
;; Reppy CML Style Channels and Events with Combinators

#+:LISPWORKS
(defun spawn (fn &key args name)
  (apply #'mp:process-run-function (symbol-name (gensym (or name "RCH:spawn-"))) nil
         fn args))

;; -----------------------------------------------------------

(define-condition no-rendezvous (error)
  ()
  (:report no-rendezvous-error))

(defun no-rendezvous-error (err stream)
  (declare (ignore err))
  (format stream "No event rendezvous"))

(define-condition timeout (error)
  ()
  (:report timeout-error))

(defun timeout-error (err stream)
  (declare (ignore err))
  (format stream "Timeout"))

;; ----------------------------------------------------------------------
;; CHANNEL -- the object of a rendezvous between threads. Channel
;; objects are shared between threads, and so must be protected by a
;; lock when reading/writing them.

(defvar *comm-id*  (make-ref 0)) ;; global ID counter

#+:LISPWORKS
(progn
  ;; LW has a strong enough GC finalization protocol that it can deal
  ;; directly with core objects. No need for the handle indirection
  ;; and object-display seen in SBCL.
  
  (defstruct (channel
              (:constructor %make-channel))
    (id       (atomic-incf *comm-id*)) ;; key for channel-refs
    (valid    t) ;; i.e., not discarded, in active service
    (readers  (make-lf-queue))
    (writers  (make-lf-queue)))
  
  (defun make-channel ()
    (let ((ch (%make-channel)))
      (hcl:add-special-free-action 'finalize)
      (hcl:flag-special-free-action ch)
      ch))

  (defmethod finalize ((ch channel))
    (release-resource ch)
    ch)

  (defmethod finalize (obj)
    obj)

  (defmethod release-resource :before ((ch channel) &key &allow-other-keys)
    (discard-channel ch))

  ; ------------------------------------------------
  ;; Channel-otable -- a sparse collection of channels that have been
  ;; allocated. This is like an object display with weak pointers and
  ;; is used to help GC automatically prod waiting threads on dead or
  ;; dying channels.
  
  (defvar *channel-otable*  (make-hash-table
                             :weak-kind :value))

  ;; channel-refs enable us to unify the use of channels and indirect
  ;; refs. Pass a channel-ref to a thread if there is a possibility
  ;; that you want the GC to handle a stuck thread on a channel with
  ;; an ephemeral binding in a lexical context.
  ;;
  ;; Q: Should make-channel just always automatically create a
  ;; channel-ref and return that, instead of an actual channel?
  ;;
  ;; A: No, that probably goes too far. The weakness of the
  ;; channel-otable could have extant channels disappearing on us. As
  ;; long as there is at least one direct channel binding the channel
  ;; will not be discarded by the GC. There is a fine line to discern
  ;; between using direct channel bindings and channel-ref bindings.
  ;; Can we simplify this decision process?
  
  (defstruct (channel-ref
              (:constructor %make-channel-ref))
    handle)
  
  (defmethod make-channel-ref ((ch channel))
    (let ((key (channel-id ch)))
      (prog1
          (%make-channel-ref
           :handle  key)
        (setf (gethash key *channel-otable*)  ch))))

  (defmethod make-channel-ref ((chref channel-ref))
    ;; in case user asks for a reference on what is already a reference
    chref)

  (defmethod release-resource :after ((ch channel) &key &allow-other-keys)
    ;; release-resource is called by GC finalization
    (remhash (channel-id ch) *channel-otable*))
  
  (defmethod reify-channel ((ch channel))
    ch)

  (defmethod reify-channel ((chref channel-ref))
    (gethash (channel-ref-handle chref) *channel-otable*))
  
  ) ;; LISPWORKS

;; -----------------------------------------------------------------------
;; COMM-CELL -- one of these belongs to each thread attempting a
;; rendezvous.  These cells may become enqueued on pending
;; reader/writer queues in channels.  And with composable events the
;; cell may become enqueued on more than one channel, which any one of
;; them might successfully rendezvous.
;;
;; The first successful rendezvous claims the day. So on an SMP
;; processor we need to grab claims using atomic operators. We could
;; also use locks, but that seems too heavy handed.

(defstruct comm-cell
  ;; numerical sequence number for MCAS ordering
  (id          (atomic-incf *comm-id*))
  
  ;; NIL if not yet performed, will contain the leaf BEV which fired
  ;; against this cell. I.e., in which leaf did the rendevouz occur?
  ;; Note: nowhere in this object is there any indication of what
  ;; counterparty was involved in the rendezvous. (Good for security.
  ;; Necessary?)
  (performed   (make-ref nil))

  ;; needs-wait will be true if we are ever placed on a R/W queue and
  ;; need to wait for a rendezvous.
  (needs-wait  nil)

  ;; calling proc, some way to reach the thread that owns this object.
  (owner       (mp:make-mailbox))

  ;; the comm data value
  data

  ;; count of channels on which this object is currently enqueued
  ;; NOTE: this might show as zero, even if it had been placed on one
  ;; or more channels. SMP multithreading might have popped it off
  ;; those queues at any time.  The needs-wait slot will remain T.
  (refct       (make-ref 0)))

;; ------------------

(defun comm< (comm1 comm2)
  #F
  (declare (comm-cell comm1 comm2))
  (< (comm-cell-id comm1)
     (comm-cell-id comm2)))

(declaim (inline comm<))

;; ====================================================================

(defun mark (comm bev)
  ;; the comm might also be on other channels and might have been already marked
  ;; returns t if successfully marked, nil otherwise
  ;; interrupts should be disabled before calling this function
  ;; returns t if we marked, nil if not
  #F
  (declare (comm-cell comm))
  (mcas1 (comm-cell-performed comm) nil bev))

(defun marked? (comm)
  ;; return non-nil if already marked
  ;; interrupts should be disabled before calling this function
  #F
  (declare (comm-cell comm))
  (mcas-read (comm-cell-performed comm)))

(defun mark2 (comm1 bev1 comm2 bev2)
  #F
  (declare (comm-cell comm1 comm2))
  ;; interrupts should be disabled before calling this function
  ;; we know we won't succeed when (eq comm1 comm2)
  (unless (eq comm1 comm2)
    ;; to prevent deadlocks, always acquire in ascending ID order
    (unless (comm< comm1 comm2)
      (rotatef comm1 comm2)
      (rotatef bev1  bev2))
    (mcas `((,(comm-cell-performed comm1) nil ,bev1)
            (,(comm-cell-performed comm2) nil ,bev2)))
    ))

;; ====================================================================

(defun refct (comm)
  #F
  (declare (comm-cell comm))
  (atomic-value (comm-cell-refct comm)))

(defun incref (comm)
  #F
  (declare (comm-cell comm))
  (atomic-incf (comm-cell-refct comm)))

(defun decref (comm)
  #F
  (declare (comm-cell comm))
  (atomic-decf (comm-cell-refct comm)))

;; ----------------

(defun prod-owner (comm bev)
  (mp:mailbox-send (comm-cell-owner comm) bev))

(defun wait-for-comm-signaled (comm)
  (mp:mailbox-read (comm-cell-owner comm)))

;; ------------------------------------

(defstruct comm-tuple
  ;; the structure of tuples stored in channel queues
  comm async bev data)

(defun dequeue-tuple (queue)
  (um:when-let (tup (lf-queue-read queue))
    (decref (comm-tuple-comm tup))
    tup))

(defun enqueue-tuple (queue tup)
  (declare (comm-tuple tup))
  (incref (comm-tuple-comm tup))
  (lf-queue-send queue tup)
  tup)

;; -----------------------------------------------------------------------
;; COMM-EVENT -- a basic component that represents every kind of
;; composable event. Each kind of event is distinguished by its
;; behavior (BEV).

(defstruct comm-event
  behavior)

(defun get-leafs (evt comm &optional leafs wlst alst)
  ;; leafs, wlst, and alst are accumulators that will hold a list of
  ;; leaf events, wrap clauses, and abort clauses, presenting their
  ;; accumulations to lower level event nodes in the event tree.
  ;;
  ;; These lists are presented to the leaf nodes to inform them of the
  ;; surrounding clauses along the path descending from the top of the
  ;; event tree to their location.
  ;;
  ;; On successful rendezvous a leaf event will be asked to remove the
  ;; wrap-abort clauses along its success path, and fold the layers of
  ;; wrap clauses around its data result.
  ;;
  ;; All unsuccessful leaf events will be asked to perform their
  ;; wrap-aborts.
  ;;
  ;; The lists become embedded in the leaf closures.
  ;;
  (if evt  ;; embedded nulls might happen, c.f., choose
      (funcall (comm-event-behavior evt) comm leafs wlst alst)
    ;; else
    leafs))

;; -----------------------------------------------------------------------

(defun leaf-result (ans wlst)
  ;; apply the wrappers along the path to the top of the tree
  (reduce #'(lambda (ans fn)
              (funcall fn ans))
          wlst
          :initial-value ans))

(defun leaf-kill-aborts (alst)
  ;; Kill off any wrap-aborts along the path to the top of the tree.
  ;;
  ;; Calling a wrap-abort function asks it for its abort executive
  ;; function.  These are supposed to be once-only items, and so by
  ;; asking, we neutralize them for later queries.
  (um:foreach #'funcall alst))

(defun leaf-abort (alst)
  ;; fire off all the wrap-abort clauses along the way
  ;; back to the top of the tree.
  (dolist (bev alst)
    ;; if behavior returns a function for cleanup
    ;; fire that function off on its own thread
    ;; to keep our time bounded. That function could fail on its own.
    (um:when-let (fn (funcall bev))
      ;; it is assumed here that the abort wrappers terminate, and do
      ;; so without invoking subsequent funcall-async - otherwise a
      ;; deadlock could ensue. If likely, then have fn spawn an async
      ;; thread for itself.
      (um:dispatch-parallel-async fn))
    ))

;; BEVs = Behaviors
(defmacro make-leaf-behavior ((comm leafs wlst alst &key self)
                              &key ans poll kill-aborts result abort)
  ;; leaf events are the only ones capable of communicating across channels
  `#'(lambda (,comm ,leafs ,wlst ,alst)
       (um:letrec ((,self  (um:dlambda
                             (:poll ()
                              ,poll)
                             (:result ()
                              ,(or result
                                   `(leaf-result ,ans ,wlst)))
                             (:abort  ()
                              ,(or abort
                                   `(leaf-abort ,alst)))
                             (:kill-aborts ()
                              ,(or kill-aborts
                                   `(leaf-kill-aborts ,alst)))
                             )))
         (cons ,self ,leafs))))

;; -----------------------------------------------------------------------
;; dlambda functions

(defun poll (bev)
  ;; every leaf node has a :poll routine
  (funcall bev :poll))

(defun get-result (bev)
  ;; every leaf node has a :result routine
  (funcall bev :result))

(defun do-abort (bev)
  ;; every leaf node has a :abort routine
  (funcall bev :abort))

(defun kill-aborts (bev)
  ;; every leaf node has a :kill-aborts routine
  (funcall bev :kill-aborts))

;; -----------------------------------------------------------------------
;; There are two dimensions of SMP multiple access here...  The first
;; is against the channel, and the second is against comm-cel objects.
;; Channels are shared among threads and comm-cells are shared among
;; channels.
;;
;; We use a multiple-CAS (MCAS) protocol on the comm-cells to allow
;; unrestricted and paired marking attempts by all threads. A
;; successful pair marking denotes a rendezvous.
;;
;; But to prevent race conditions we use a CAS spin-lock on the
;; channel objects when we are polling and possibly stuffing or
;; clearing the reader/writer queues.  As a result we can get by with
;; a very fast and simple single-threaded queue implementation.
;;
;; Note that because of our MCAS marking protocol, simultaneous
;; send/recv on a channel, in either order, works okay, i.e., we do
;; not self-trigger, except in the case of recv after poke. Hence we
;; behave as would be desired.
;;

(defun find-eligible-tuple (queue eligible-p)
  #F
  ;; Scan a queue for an eligbible tuple and discard marked tuples
  ;; from the queue. An eligible tuple may become marked from
  ;; eligible-p. This version avoids consing.
  (let (ans)
    (lf-queue-stuff queue
                    (delete-if #'(lambda (tup)
                                   (declare (comm-tuple tup))
                                   (let ((comm (comm-tuple-comm tup)))
                                     (cond ((and (null ans)
                                                 (funcall eligible-p tup))
                                            (setf ans tup)
                                            (decref comm)
                                            t)
                                           
                                           ((marked? comm)
                                            (decref comm)
                                            t)
                                           
                                           (t nil)
                                           )))
                               (the list (lf-queue-contents queue))))
    ans))

(defun do-polling (ch queue my-comm my-bev rendezvous-fn blocking-fn)
  #F
  (declare (channel ch))
  ;; such strong similarities between reader & writer polling that
  ;; they should share a common core code
  (when (channel-valid ch) ;; return nil if discarded channel
    ;; first, post our queue entry as a declaration of intent
    (let ((blkans  (funcall blocking-fn)))
      ;; now check if we can immediately rendezvous
      (um:if-let (tup (find-eligible-tuple queue
                                           #'(lambda (tup)
                                               (declare (comm-tuple tup))
                                               (with-accessors ((other-comm comm-tuple-comm)
                                                                (other-bev  comm-tuple-bev)) tup
                                                 ;; tuple is eligible if we can mark both them and us
                                                 (mark2 other-comm other-bev my-comm my-bev))
                                               )))
          (locally
            (declare (comm-tuple tup))
            (with-accessors ((other-comm  comm-tuple-comm)
                             (other-async comm-tuple-async)
                             (other-bev   comm-tuple-bev)) tup
              (funcall rendezvous-fn tup)
              (unless other-async
                (prod-owner other-comm other-bev))
              my-bev)) ;; indicate successful rendezvous
        
        ;; else - just have to wait on this one
        blkans))
    ))

;; -----------------------------------------------------------------

(defun sendEvt (ch data &key async)
  (make-comm-event
   :behavior
   (make-leaf-behavior (wcomm leafs wlst alst :self me)
     ;; SendEvt behavior -- scan the channel pending readers to see if
     ;; we can rendezvous immediately. If not, then enqueue us on the
     ;; pending writers for the channel.
     :ans   data
     :poll  (um:when-let (ch (reify-channel ch))
              (locally
                (declare (channel ch)
                         (comm-cell wcomm))
                (do-polling ch (channel-readers ch) wcomm me
                            
                            #'(lambda (tup) ;; rendezvous
                                #F
                                (declare (comm-tuple tup))
                                (with-accessors ((rcomm  comm-tuple-comm)) tup
                                  (declare (comm-cell rcomm))
                                  (setf (comm-cell-data rcomm) data)))
                            
                            #'(lambda () ;; blocking
                                #F
                                ;; -----------------------------------------------
                                ;; When no pending reads, enqueue the write.
                                ;; Return nil to indicate we did not rendezvous.
                                (unless async
                                  (setf (comm-cell-needs-wait wcomm) t))
                                (enqueue-tuple (channel-writers ch)
                                               (make-comm-tuple
                                                :comm  wcomm
                                                :async async
                                                :bev   me
                                                :data  data))
                                (when async ;; indicate that we did, kinda, sorta, rendezvous
                                  me))
                            )))
     )))

;; -----------------------------------------------------------------

(defun recvEvt (ch &key async)
  (make-comm-event
   :behavior
   (make-leaf-behavior (rcomm leafs wlst alst :self me)
     ;; recvEvt behavior -- scan the channel pending writers to see if
     ;; we can rendezvous immediately. If not, then enqueue us on the
     ;; channel pending readers.
     :ans  (comm-cell-data rcomm)
     :poll (um:when-let (ch (reify-channel ch))
             (locally
               (declare (channel ch)
                        (comm-cell rcomm))
               (do-polling ch (channel-writers ch) rcomm me
                           
                           #'(lambda (tup) ;; rendezvous
                               #F
                               (declare (comm-tuple tup))
                               (with-accessors ((data  comm-tuple-data)) tup
                                 (setf (comm-cell-data rcomm) data)))
                           
                           #'(lambda () ;; blocking
                               #F
                               (unless async
                                 (setf (comm-cell-needs-wait rcomm) t)
                                 (enqueue-tuple (channel-readers ch)
                                                (make-comm-tuple
                                                 :comm  rcomm
                                                 :async async
                                                 :bev   me))
                                 nil)) ;; indicate failure to rendezvous
                           )))
     )))

;; -----------------------------------------------------------------

(defun alwaysEvt (data)
  (make-comm-event
   :behavior
   (make-leaf-behavior (comm leafs wlst alst :self me)
     ;; alwaysEvt behavior -- always successfully rendezvous if we
     ;; haven't already rendevoused elsewhere.
     :ans  data
     :poll (mark comm me)
     ;; The comm might also be on other channels and might have been
     ;; marked. Either way, we successfully polled but allow other mark
     ;; to take effect
     )))

(defun nonEvt ()
  ;; an event which serves to create a NIL COMM-EVENT object
  ;; .. useful as a nil-stub in a situation like this:
  ;;
  ;;       (choose (recvEvt ch)
  ;;               (if timeout
  ;;                   (timeoutEvt timeout)
  ;;                 (nonEvt)))
  ;;
  ;; -- no longer needed, just use nil...
  nil)

;; -----------------------------------------------------
;; Reppy Combinators
;;

(defun get-list-of-leafs (comm evs leafs wlst alst)
  ;; result is returned in reverse order. But so are the surrounding
  ;; elements. To get CHOOSE* to behave properly you need to reverse the
  ;; order of the grand list before polling. It won't do any good to
  ;; perform that reversal here...
  (reduce #'(lambda (leafs ev)
              (get-leafs ev comm leafs wlst alst))
          evs
          :initial-value leafs))
  
(defun choose* (&rest evs)
  ;; an event that provides for deterministic ordering of alternative
  ;; event rendezvous (c.f., CHOOSE). Only one event can rendezvous.
  ;;
  ;; While it is possible that none can immediately rendezvous, and we
  ;; may be placed on a pending reader/writer queue, the ordering
  ;; allows for early rendezvous via first scan polling. Ordering may
  ;; be preferential here.
  ;; (assert (every #'comm-event-p evs))
  (make-comm-event
   :behavior
   #'(lambda (comm leafs wlst alst)
       (get-list-of-leafs comm evs leafs wlst alst))
   ))

;; --------------------------------------------------

(defun scramble-list (lst)
  (um:nlet-tail iter ((hd nil)
		   (tl nil)
		   (lst lst))
    (if (endp lst)
        (nconc hd tl)
      ;; else
      (if (zerop (random 2))
          (iter (cons (car lst) hd) tl (cdr lst))
        (iter hd (cons (car lst) tl) (cdr lst)))
      )))

(defun choose (&rest evs)
  ;; a CHOOSE event provides a nondeterministic ordering of
  ;; alternative event rendezvous (c.f. choose*). Only one event can
  ;; rendezvous.
  ;;
  ;; While it is possible that none can immediately rendezvous, and we
  ;; may be placed on a pending reader/writer queue, the ordering
  ;; allows for early rendezvous via first scan polling. But ordering
  ;; is random here, giving no particular preference to any of the
  ;; alternative events.
  ;; (assert (every #'comm-event-p evs))
  (make-comm-event
   :behavior
   #'(lambda (comm leafs wlst alst)
       (get-list-of-leafs comm (scramble-list evs) leafs wlst alst))
   ))

;; --------------------------------------------------

;; destructive read cell
(defun once-ref (val)
  #'(lambda ()
      (shiftf val nil)))

;; --------------------------------------------------

(defun wrap-abort (ev fn)
  ;; fn should be a thunk (function of no args).  if used after a
  ;; nonsignaled channel, it will be spawned to protect the executive.
  (make-comm-event
   :behavior
   #'(lambda (comm leafs wlst alst)
       (let ((bev (once-ref fn))) ;; funcalling bev returns fn, but only the first time.
         ;; why the indirection? We need to break cycles in the abort
         ;; graph, where more than one leaf might have a given abort
         ;; clause along the path to the root of the event tree. We only
         ;; want the abort clause to fire once.
         (get-leafs ev comm leafs wlst (cons bev alst))))
   ))

;; --------------------------------------------------

(defun fail (ev)
  ;; fail ensures that if any wrapped events rendezvous, they will
  ;; be seen as failing to rendezvous, even as they return the value
  ;; from the rendezvous.  Wrappers on the rendezvous value will be
  ;; respected, but no abort clauses will be neutralized.
  (make-comm-event
   :behavior
   #'(lambda (comm leafs wlst alst)
       (declare (ignore alst))
       (get-leafs ev comm leafs wlst nil))
   ))

(defun abortEvt (&optional val)
  (fail (alwaysEvt val)))

;; --------------------------------------------------

(defun wrap (ev fn)
  ;; fn should expect one argument.  A failed channel operation
  ;; generally provides a NIL argument.  Other events (should)
  ;; produce something else. If you need to pass a NIL, you
  ;; might encase it in a list for distinction from the failed
  ;; rendezvous case.
  ;;
  ;; ... this is rather like an :AFTER method on the rendezvous
  ;; result. The returned value from a wrapped rendezvous is the
  ;; result produced by the function fn. If this wrapped event is not
  ;; chosen, the function will not be asked to perform.
  ;;
  (make-comm-event
   :behavior
   #'(lambda (comm leafs wlst alst)
       (get-leafs ev comm leafs (cons fn wlst) alst))
   ))

;; --------------------------------------------------

(defun guard (fn)
  ;; a guarded function must return another COMM-EVENT
  ;; ... rather like an around handler on embedded events
  ;; The fn is only executed if this event is chosen.
  (make-comm-event
   :behavior
   #'(lambda (comm leafs wlst alst)
       (get-leafs (funcall fn) comm leafs wlst alst))
   ))

;; --------------------------------------------------

(defun timerEvt (dt &key absolute)
  ;; an event that rendezvous with a timer
  ;; dt will be a relative time unless :absolute is T
  (cond ((null dt) ;; no time indicated -- just form a nop event
         (nonEvt))

        ((not (realp dt))
         (error "timerEvt requires a numeric dt value: ~A" dt))

        ((not (plusp dt))
         (alwaysEvt t)) ;; should already have fired

        (t ;; anything else is a real timer rendezvous
           (guard #'(lambda ()
                      (let* ((ch     (make-channel))
                             (timer  (make-timer #'poke ch t)))
                        (schedule-timer timer dt :absolute absolute)
                        (wrap-abort (recvEvt ch)
                                    #'(lambda ()
                                        (unschedule-timer timer)))
                        ))))
        ))

;; ----------------------------------------------------------------------------------
;; Events that could generate errors if chosen

(defun wrap-error (ev errfn)
  ;; An event that fires an error if successful rendezvos. It also
  ;; acts like a failed rendezvous. Errfn should be prepared to accept
  ;; one argument, which is the result of the ev rendezvous.
  (fail (wrap ev
              #'(lambda (v)
                  (error (funcall errfn v)))
              )))

(defun errorEvt (errfn)
  ;; An event that always fails with an error. Doing it this way,
  ;; instead of just presenting an immediate error allows the error to
  ;; be bypassed if this event is never chosen. The error in the
  ;; wrapped function only occurs on successful rendezvous with this
  ;; event.
  (wrap-error (alwaysEvt nil)
              errfn))

(defun timeoutEvt (dt)
  ;; an event that produces a timeout error
  (wrap-error (timerEvt dt)
              #'(lambda (arg)
                  (declare (ignore arg))
                  (make-condition 'timeout))))

(defun wrap-timeout (dt ev)
  (choose* ev
           (timeoutEvt dt)))

;; ----------------------------------------------------------------------------

#+:LISPWORKS
(defun discard-channel (ch)
  ;; (print "Discarding channel")
  (when (shiftf (channel-valid ch) nil) ;; if was valid?
    (labels
        ((prod (q)
           (dolist (tup (lf-queue-contents q))
             (with-accessors ((comm  comm-tuple-comm)
                              (async comm-tuple-async)) tup
               (declare (ignore _))
               (decref comm)
               (when (and (zerop (refct comm))   ;; not on any other queues
                          (mark comm t)          ;; hasn't already rendezvous
                          (not async))           ;; not an async event
                 ;; since there should be no contention for
                 ;; the comm object, (i.e., the channel has
                 ;; been discarded and so no possibility of
                 ;; rendezvous) we use the mark operation
                 ;; merely to check whether or not it has
                 ;; already rendezvousd.
                 (prod-owner comm nil))   ;; prod with rendezvous failure
               ))))
      (prod (channel-readers ch))
      (prod (channel-writers ch))
      )))

(defun reset-channel (ch)
  (discard-channel ch)
  (setf (channel-valid ch) t))

;; --------------------------------------------------
;; user level event composition

(defun sync (ev)
  ;; reify abstract events to produce a rendezvous
  (let* ((comm    (make-comm-cell))
         (allevts (nreverse (get-leafs ev comm)))  ;; all possible event sources
         ansbev)
    
    (labels ((select-event ()
               ;; obtain the first event that fires returns the BEV of the winning
               ;; rendezvous, or NIL on rendezvous failure.
               (or (poll-leaf-events)
                   (wait-for-event)))

             (poll-leaf-events ()
               (some #'poll allevts))

             (wait-for-event ()
               (if (comm-cell-needs-wait comm)
                   (wait-for-comm-signaled comm)
                 ;; else - for async events, maybe we rendezvous during polling?
                 (marked? comm)))

             (perform-aborts ()
               ;; discard-channel may have produced a no-rendezvous (null ansbev)
               (when ansbev
                 ;; first, kill off any wrap-aborts along the rendezvous path
                 (kill-aborts ansbev))
               ;; then for all other events, perform the wrap-aborts
               (um:foreach #'do-abort allevts)))
      
      ;; at this point we have the tree of BEVs and are ready to initiate
      ;; the sync action. We need to guarantee the abort cleanups.
      
      (safe-unwind-protect
       (setf ansbev (select-event))
       (perform-aborts))
      (if ansbev
          (get-result ansbev)
        (error (make-condition 'no-rendezvous)))
      )))

(defun send (ch item)
  ;; send an item and wait for a matching recv to pick it up
  (sync (sendEvt ch item)))

(defun recv (ch)
  ;; wait for an item on channel
  (sync (recvEvt ch)))

(defun pokeEvt (ch item)
  (sendEvt ch item :async t))

(defun poke (ch item)
  ;; send the item, but don't wait for a matching recv
  (sync (pokeEvt ch item)))

(defun peekEvt (ch &optional default)
  ;; Allows us to wrap and wrap-abort. By using an abortEvt in
  ;; parallel with the recvEvt we fire off any parallel wrap-aborts on
  ;; an unsuccessful polling
  (choose* (recvEvt ch :async t)
           (abortEvt default)))

(defun peek (ch &optional default)
  ;; return item on channel or default value, but don't wait
  (sync (peekEvt ch default)))

(defun select (&rest evl)
  ;; random application of choice
  (sync (apply #'choose evl)))

(defun select* (&rest evl)
  ;; ordered application of choice
  (sync (apply #'choose* evl)))

;; ---------------------------------------------------------
;; Safe access of a channel shared through argument passing...
;;
;; When a channel is sent to another thread it is possible that the
;; channel will become abandoned by the sender, leaving the thread to
;; fend for itself. And we want to be able to scavenge abandoned
;; threads with GC finalization on the abandoned channel. So the only
;; way to have that work is by assuring that no stray references to a
;; channel are left lying around.
;;
;; By passing a once-only ref to a shared channel, we both allow for
;; the GC scavenging (via once-ref), and by using these safe accessors
;; on the shared channel, we gracefully manage the possibility that
;; the channel will have been abandoned and we should not pay for an
;; error of type no-rendezvous.
;;
;; However, if the thread needs to know that the channel was
;; abandoned, as with an RPC with transactional roll-back, then you
;; should not call these safe-xxx routines, and you should manage the
;; errors for yourself.
;;
;; By curiosity of the implementation, I found that simply passing a
;; (list ch) indirect reference is insufficient to enable GC
;; scavenging when combined with handler-case or ignore-errors.
;; Instead, the indirect reference needs to be something not easily
;; transformed into stack-local references by the compiler optimizer.
;; And to achieve that we make use of the once-ref functional closure
;; mechanism shown here.
;;
;; Note that to achieve GC scavenging, a reference to the channel must
;; be used. You cannot allow the thread function to capure the channel
;; from its lexical environment as in:
;;
;;   (let ((ch (make-channel)))
;;       (spawn (lambda ()
;;                 (do-something-with ch)))
;;    ... )
;;
;; The reason for this is that the lexical binding in the functional
;; closure remains, preventing GC scavenging. Instead, you must pass
;; an indirect reference to the channel, either through argument
;; passing, or by a lexical capture. And that reference must be a
;; self-erasing version (or be manually erased) so that it removes any
;; vestigial channel reference after final access.

(defun safe-poke (ch val)
  (handler-case
      ;; we might get a no-rendevous error if the channel has been
      ;; scavenged by the GC as a result of sender getting diverted
      ;; with another rendevous instead of this one.
      (poke ch val)
    (no-rendezvous ())))

(defun safe-send (ch val)
  (handler-case
      (send ch val)
    (no-rendezvous ())))

(defun safe-peek (ch &optional default)
  ;; this one probably isn't needed, but there is an implicit race
  ;; condition in not doing so
  (handler-case
      (peek ch default)
    (no-rendezvous ())))

(defun safe-recv (ch)
  (handler-case
      (recv ch)
    (no-rendezvous ())))

;; ---------------------------------------------------------

(defun execEvt (fn &rest args)
  ;; turn any function execution into an event
  ;; so that we can have execution with timeouts
  (guard #'(lambda ()
             (let* ((ch  (make-channel))
                    (thr (spawn #'(lambda ()
                                    (poke ch (apply #'um:capture-ans-or-exn fn args)))
                                )))
               (wrap-abort
                (wrap (recvEvt ch)
                      #'um:recover-ans-or-exn)
                #'(lambda ()
                    ;; this respects unwind-protect in the thread
                    (mp:process-terminate thr)))
               ))
         ))

(defun exec-with-timeout (dt fn &rest args)
  (sync (wrap-timeout dt (apply #'execEvt fn args))))

;; ---------------------------------------------------------

(defun dispatch-serial-evt (q fn &rest args)
  (apply #'execEvt #'um:dispatch-serial-sync q fn args))

(defun dispatch-parallel-evt (fn &rest args)
  (apply #'execEvt #'um:dispatch-parallel-sync fn args))

(defun wait-for-dispatch-group-evt (grp)
  (execEvt #'um:wait-for-dispatch-group grp))

(defun wait-for-serial-dispatch-evt (q)
  (execEvt #'um:wait-for-serial-dispatch q))

(defun wait-for-parallel-dispatch-evt ()
  (execEvt #'um:wait-for-parallel-dispatch))

;; ---------------------------------------------------------

(defun joinEvt (thread)
  ;; an event that waits until thread finishes, dies, is terminated,
  ;; or is already dead
  (execEvt #'(lambda ()
               (join-thread thread))
           ))

(defun flushInp (stream)
  (do ()
      ((null (read-char-no-hang stream nil nil)))
    ))

(defun keyEvt (&key flush (stream *standard-input*))
  ;; an event that waits on a keyboard character
  (execEvt #'(lambda ()
               (when flush
                 (flushInp stream))
               (read-char stream))))

(defun lineEvt (&key flush (stream *standard-input*))
  ;; an event that waits on a line of input from the keyboard
  (execEvt #'(lambda ()
               (when flush
                 (flushInp stream))
               (read-line stream))))

(defun sexpEvt (&key flush (stream *standard-input*) read-eval)
  ;; an event that waits for a SEXP of input from the keyboard
  (execEvt #'(lambda ()
               (when flush
                 (flushInp stream))
               (let ((*read-eval* read-eval))
                 (read stream)))
           ))

;; ----------------------------------------------------------------------------

(defun sendEvt-with-nack (ch ans commitFn abortFn)
  ;; a server-side sendEvt that will perform the commitFn on
  ;; successful rendezvous, or the abortFn on any sort of abort
  ;; condition, or when the client sends a nack across the channel
  ;; instead of reading our sent value
  (wrap-abort (choose* (wrap (sendEvt ch ans)
                             #'(lambda (arg)
                                 (declare (ignore arg))
                                 (funcall commitFn)) )
                       ;; if we recv anything on the replyCh it is taken
                       ;; as an abort indication
                       (wrap (recvEvt ch)
                             #'(lambda (arg)
                                 (declare (ignore arg))
                                 (funcall abortFn)) ))
              abortFn)) ;; might happen if client discarded the channel

(defun recvEvt-with-nack (ch)
  ;; a client-side recvEvt that will poke the channel with a nack in
  ;; case of abort
  (wrap-abort (recvEvt ch)
              #'(lambda ()
                  (poke ch nil))
              ))

(defun do-nothing (&rest args)
  args)
  
(defun rpcEvt (serverCh req &key
			      timeout
                              (replyCh (make-channel)))
  ;; client side composable event for RPC interactions
  (wrap-timeout timeout
   (guard #'(lambda ()
              (poke serverCh (list req replyCh))
              (recvEvt-with-nack replyCh)
              ))))

(defun rpc? (ch req &rest args)
  ;; args could be :timeout, :replyCh
  ;; client-side RPC call
  (sync (apply #'rpcEvt ch req args)))

(defun rpc! (replyCh ans &key
                     (commitFn #'do-nothing commitFn-p) ;; should be a thunk
                     (abortFn #'do-nothing  abortFn-p))  ;; should be a thunk
  ;; server-side RPC reply
  (handler-case
      ;; protect us from timeouts, transaction aborts, and discarded
      ;; reply channels
      (if (or abortFn-p commitFn-p)
          ;; for non-idempotent transactions
          (sync (sendEvt-with-nack replyCh ans commitFn abortFn))
        ;; else - be quick
        (poke replyCh ans))
    
    ;; error handlers
    (no-rendezvous ())  ;; could happen if channel was discarded
    ))

;; ---------------------------------------------------------

(defmacro with-channel (ch &body body)
  `(let ((,ch (make-channel)))
     (unwind-protect
         (progn
           ,@body)
       (release-resource ,ch))))

(defmacro with-channels (chlist &body body)
  `(let ,(mapcar #1`(,a1  (make-channel)) chlist)
     (unwind-protect
         (progn
           ,@body)
       (um:foreach #'release-resource (list ,@chlist))
       )))

;; ---------------------------------------------------------

#| ;; test of the new dual-use ReplyCh protocol
(progn
(defun make-demo-server2 (ch)
  (spawn #'(lambda ()
             (loop for msg = (handler-case
                                 (recv ch)
                               (no-rendezvous ()
                                 (loop-finish)))
                   while msg
                   do
                   (destructuring-bind (req replyCh) msg
                     (um:dcase req
                       (:primes () (rpc! replyCh '(1 2 3 5 7 "9?")))
                       (:time   () (rpc! replyCh (get-universal-time)))
                       (:slow   (dt)
                        (sleep dt)
                        (rpc! replyCh "I am slow"
                              :commitFn #'(lambda ()
                                            (declare (ignore arg))
                                            (print "I made it!"))
                              :abortFn #'(lambda ()
                                           (declare (ignore arg))
                                           (print "I missed..."))))
                       (:kill ()  (rpc! replyCh :okay)
                        (loop-finish))
                       (t (&rest args)
                          (declare (ignore args))
                          (rpc! replyCh "Eh?"))
                       ))))
         ))

(defun tst1 ()
  (let* ((schan  (make-channel))
         (ref    (make-channel-ref schan)))
    (make-demo-server2 ref)
    (print (rpc? schan '(:time) :timeout 2))
    ;; (print (rpc? schan '(:time)))
    (print (rpc? schan '(:primes)))
    (print (rpc? schan '(:what?)))
    (print (ignore-errors (rpc? schan '(:slow 0.996) :timeout 1)))
    ))

(defun tst1a ()
  (let* ((schan   (make-channel))
         (ref     (make-channel-ref schan))
         (replyCh (make-channel)))
    (make-demo-server2 ref)
    (print (rpc? schan '(:time) :replyCh replyCh :timeout 1))
    (print (rpc? schan '(:primes) :replyCh replyCh))
    (print (rpc? schan '(:what?) :replyCh replyCh))
    (print (ignore-errors (rpc? schan '(:slow 0.996) :replyCh replyCh :timeout 1)))
    ))

(defun tst2 ()
  (with-channel schan
    (make-demo-server2 schan)
    (print (rpc? schan '(:time) :timeout 1))
    (print (rpc? schan '(:primes)))
    (print (rpc? schan '(:what?)))
    (print (ignore-errors (rpc? schan '(:slow 0.996) :timeout 1)))
    ))

;; -----------------------------------------------------------
(defun make-demo-server-spd (ch)
  (spawn #'(lambda ()
             (loop for (req replyCh) = (handler-case
                                           (recv ch)
                                         (no-rendezvous ()
                                           (loop-finish)))
                   do (rpc! replyCh :ok)))
         ))

(defun tst-spd (&optional (n 100000))
  (with-channel schan
    ;; 4.1 microsec/rpc = 244 kRPC/sec
    (make-demo-server-spd schan)
    (time (loop repeat n do
                (rpc? schan t))
          )))
) ;; progn

|#
#|
;; Try using select to determine relative speed of assoc list vs hashtable for various sizes
;; Result: over 100M Iters, Assoc wins for fewer than 7 items, else Hashtable
(defun tst-ah (&key (nitems 15) (niters 1000000))
  (let* ((ch    (make-channel))
         (htbl  (make-hash-table))
         (keys  (remove-duplicates
                 (loop repeat (* 10 nitems) collect (random nitems))))
         (alist (loop for key in keys collect
                      (progn
                        (setf (gethash key htbl) key)
                        (list key key))))
         (plist (loop for key in keys nconc
                      (list key key)))
         (tree  (let ((tree (sets:empty)))
                  (loop for key in keys do
                        (setf tree (maps:add key key tree))))))
    (labels ((tst-assoc ()
               (loop for key from 0 below niters do
                     (assoc (mod key (* 2 nitems)) alist))
               (poke ch "Assoc"))
             (tst-htbl ()
                (loop for key from 0 below niters do
                      (gethash (mod key (* 2 nitems)) htbl))
                (poke ch "HTbl"))
             (tst-plist ()
               (loop for key from 0 below niters do
                     (getf plist (mod key (* 2 nitems))))
               (poke ch "PList"))
             (tst-tree ()
               (loop for key from 0 below niters do
                     (maps:find (mod key (* 2 nitems)) tree))
               (poke ch "Tree"))
             (time (fn)
               (let ((start (get-universal-time))
                     stop)
                 (funcall fn)
                 (setf stop (get-universal-time))
                 (- stop start))))

      #|
      (sync (guard (lambda ()
                     (spawn #'tst-assoc)
                     ;; (spawn #'tst-htbl)
                     ;; (spawn #'tst-plist)
                     (spawn #'tst-tree)
                     (recvEvt ch))))
      |#
      (list :AList (time #'tst-assoc)
            :PList (time #'tst-plist)
            :Tree  (time #'tst-tree)
            :HTbl  (time #'tst-htbl))
      )))
|#
#|
(defun xtst ()
  (let ((ch (make-channel)))
    (sync (choose* (wrap-abort (recvEvt ch)
                               (lambda ()
                                 (print "WrapAbort fired")))
                   (timeoutEvt 3)))
    ))
|#
;; ----------------------------------------------------------
;; Equivalent(?) server using async mailboxes already in Lisp...

#| -- for LispWorks only...
(progn
  (defun mbrpc? (sbox req &key timeout replyMB)
    (let ((rbox (or replyMB
                    (mp:make-mailbox))))
      (mp:mailbox-send sbox (list req rbox))
      (mp:mailbox-read rbox :rpc timeout)))
  
  (defun mbrpc! (rbox ans)
    (mp:mailbox-send rbox ans)
    ans)
  
  (defun make-mbserver (mbox)
    (spawn (lambda ()
             (loop for (req replymb) = (mp:mailbox-read mbox)
                   until (and (eq req :kill)
                              (mbrpc! replymb :OKAY))
                   do
                   (case req
                     (:primes (mbrpc! replymb '(1 2 3 5 7 "9?")))
                     (:time   (mbrpc! replymb (get-universal-time)))
                     (t       (mbrpc! replymb "Eh?"))
                     )))
           ))
  
  (defun mb-tst1 ()
    (let ((smb (mp:make-mailbox)))
      (make-mbserver smb)
      (print (mbrpc? smb :time))
      (print (mbrpc? smb :primes))
      (print (mbrpc? smb :what?))
      (print (mbrpc? smb :kill))))

  (defun make-mbserver-spd (mbox)
    (spawn (lambda ()
             (loop for (req replymb) = (mp:mailbox-read mbox)
                   until (and (eq req :kill)
                              (mbrpc! replymb :OKAY))
                   do
                   (mbrpc! replymb :ok)))
           ))
  
  (defun mb-tst-spd (&optional (count 1000000))
    (let ((sbox (mp:make-mailbox)))
      (make-mbserver-spd sbox)
      ;; 7.9 microsec/RPC = 127 kRPC/sec
      (time (loop repeat count do
                  (mbrpc? sbox :time)))
      (mbrpc? sbox :kill)))
  ) ;; progn
|#


;; ----------------------------------------------------------
;; Equivalent(?) server using async process mailboxes already in Lisp...
;; This is definitely the way to go if you can avoid SELECT

(defun pmbrpc? (proc req)
  (mp:process-send proc (list req mp:*current-process*))
  (mp:process-wait-for-event))

(defun pmbrpc! (rproc ans)
  (mp:process-send rproc ans)
  ans)

(defun make-pmbserver ()
  (spawn (lambda ()
           (loop for (req replyto) = (mp:process-wait-for-event)
                 until (and (eq req :kill)
                            (pmbrpc! replyto :OKAY))
                 do
                 (case req
                   (:primes (pmbrpc! replyto '(1 2 3 5 7 "9?")))
                   (:time   (pmbrpc! replyto (get-universal-time)))
                   (t       (pmbrpc! replyto "Eh?"))
                 )))
         ))
  
#|
(let ((srv (make-pmbserver)))
  (print (pmbrpc? srv :time))
  (print (pmbrpc? srv :primes))
  (print (pmbrpc? srv :what?))
  (print (pmbrpc? srv :kill)))

(let ((srv (make-pmbserver)))
  ;; 8.7 microsec/rpc = 111k rpc/sec
  (time (loop repeat 1000000 do
              (pmbrpc? srv :time)))
  (pmbrpc? srv :kill))
|#
|# ;; -- end of for LispWorks only --

;; -------------------------------------------------------------------------
#|
(defun tst ()
  (let ((ch  (make-channel))
        (ans (make-channel)))
    (spawn (lambda ()
             (sync (choose (wrap (recvEvt ch) (lambda (x) (send ans (list 1 x))))
                           (wrap (recvEvt ch) (lambda (y) (send ans (list 2 y))))))))
    (send ch 15)
    (recv ans)))

(defun tst ()
  (let ((ch1 (make-channel))
        (ch2 (make-channel))
        (ans (make-channel)))
    (spawn (lambda ()
             (let* ((val (select (recvEvt ch1)
                                 (recvEvt ch2)
                                 (timeoutEvt 2)
                                 )))
               (send ans (list 1 val)))))
    (spawn (lambda ()
             (let* ((val (sync (choose (recvEvt ch2)
                                      (recvEvt ch1)
                                      (timeoutEvt 2))
                              )))
               (send ans (list 2 val)))))
    (spawn (lambda () (select (sendEvt ch1 15)
                              (timeoutEvt 2))))
    (spawn (lambda () (select (sendEvt ch2 16)
                              (timeoutEvt 2))))
    (list (recv ans)
          (recv ans))
    ))
|#
