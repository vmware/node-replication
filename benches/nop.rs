// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

///! Define a Nop data-structure that doesn't do anything.
use node_replication::Dispatch;

#[derive(Debug, Default, Eq, PartialEq, Copy, Clone)]
pub struct Nop(usize);

impl Dispatch for Nop {
    type ReadOperation = ();
    type WriteOperation = usize;
    type Response = ();
    type ResponseError = ();

    fn dispatch(&self, _op: Self::ReadOperation) -> Result<Self::Response, Self::ResponseError> {
        unreachable!()
    }

    fn dispatch_mut(
        &mut self,
        _op: Self::WriteOperation,
    ) -> Result<Self::Response, Self::ResponseError> {
        Ok(unreachable!())
    }
}
