import asyncio

import carb
import omni.ext
import omni.timeline
import omni.usd


class MotionExtension(omni.ext.IExt):
    def __init__(self):
        self.e_stage_task = None  # asyncio.Task for the async stage-open
        self.e_stage_event = None  # asyncio.Event set() when stage is OPENED
        self.e_stage_subscription = None  # Stage event subscription handle
        self.e_timeline_interface = None  # Timeline interface
        self.e_timeline_subscription = None  # Timeline event subscription handle

    def on_startup(self, ext_id):
        # Allocate runtime objects here (core services are ready now)
        self.e_stage_event = asyncio.Event()
        self.e_timeline_interface = omni.timeline.get_timeline_interface()

        # 1) Subscribe to timeline events
        self.e_timeline_subscription = self.e_timeline_interface.get_timeline_event_stream().create_subscription_to_pop(
            self.on_timeline_event
        )

        # 2) Subscribe to stage events
        ctx = omni.usd.get_context()
        self.e_stage_subscription = (
            ctx.get_stage_event_stream().create_subscription_to_pop(self.on_stage_event)
        )

        # 3) Define and kick off async stage open (helper lives only in startup)
        async def f_stage(url: str):
            try:
                if ctx.get_stage():
                    await ctx.close_stage_async()

                # async open with minimal initial load
                self.e_stage_event.clear()
                await ctx.open_stage_async(
                    url, load_set=omni.usd.UsdContextInitialLoadSet.LOAD_NONE
                )

                # wait until on_stage_event signals OPENED
                try:
                    await asyncio.wait_for(self.e_stage_event.wait(), timeout=120.0)
                except asyncio.TimeoutError:
                    carb.log_warn("[motion.extension] Timeout waiting for Stage OPENED")

            except asyncio.CancelledError:
                carb.log_debug("[motion.extension] f_stage cancelled")
                return
            except Exception as exc:
                carb.log_error(f"[motion.extension] Stage open failed: {exc!r}")

        # schedule the open task and keep a handle for shutdown cancellation
        self.e_stage_task = asyncio.create_task(
            f_stage("file:///storage/node/scene.usd")
        )

        # 4) Register per-step callback; fires during play
        self.e_timeline_interface.add_callback("motion_step", self.motion_step)

    def on_shutdown(self):
        # Teardown in strict reverse order of startup

        # 1) Remove per-step callback first
        try:
            if self.e_timeline_interface:
                self.e_timeline_interface.remove_callback("motion_step")
        except Exception:
            pass

        # 2) Cancel stage-open task
        if self.e_stage_task:
            try:
                self.e_stage_task.cancel()
            except Exception:
                pass
            finally:
                self.e_stage_task = None

        # 3) Unsubscribe from stage events
        try:
            if self.e_stage_subscription:
                self.e_stage_subscription.unsubscribe()
        except Exception:
            pass
        finally:
            self.e_stage_subscription = None

        # 4) Unsubscribe from timeline events
        try:
            if self.e_timeline_subscription:
                self.e_timeline_subscription.unsubscribe()
        except Exception:
            pass
        finally:
            self.e_timeline_subscription = None

        # 5) Drop event and interface refs last
        self.e_stage_event = None
        self.e_timeline_interface = None

    def on_stage_event(self, e):
        if e.type == omni.usd.StageEventType.OPENED:
            carb.log_info("[motion.extension] Stage OPENED")
            if self.e_stage_event and not self.e_stage_event.is_set():
                self.e_stage_event.set()
            # auto-start simulation
            if self.e_timeline_interface and not self.e_timeline_interface.is_playing():
                self.e_timeline_interface.play()

    def on_timeline_event(self, e):
        if e.type == omni.timeline.TimelineEventType.PLAY:
            carb.log_info("[motion.extension] Timeline PLAY")
        elif e.type == omni.timeline.TimelineEventType.STOP:
            carb.log_info("[motion.extension] Timeline STOP")

    def motion_step(self, dt: float):
        carb.log_debug(f"[motion.extension] motion_step dt={dt}")
